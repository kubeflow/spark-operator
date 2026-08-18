/*
Copyright 2024 The Kubeflow authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package webhook

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/url"
	"reflect"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:admissionReviewVersions=v1,failurePolicy=fail,groups=sparkoperator.k8s.io,matchPolicy=Exact,mutating=false,name=validate-sparkapplication.sparkoperator.k8s.io,path=/validate-sparkoperator-k8s-io-v1beta2-sparkapplication,reinvocationPolicy=Never,resources=sparkapplications,sideEffects=None,verbs=create;update,versions=v1beta2,webhookVersions=v1

type SparkApplicationValidator struct {
	client client.Client

	enableResourceQuotaEnforcement bool

	// enableURLSchemeValidation gates the fetch-field URL-scheme check. It is strictly opt-in
	// (default off): when false, validateURLSchemes is not run at all and admission behaviour is
	// unchanged from before the check existed, so upgrading operators that already submit remote
	// deps are never broken. Operators turn it on to harden against operator-privileged SSRF.
	enableURLSchemeValidation bool
	// Remote values in fetch-capable spec fields must match both configured scheme and host policy.
	// Empty host lists deny all remote URLs. Local paths, file://, and local:// remain allowed.
	allowedURLSchemes       map[string]struct{}
	allowAllURLHostsSchemes map[string]struct{}
	allowedURLHosts         map[string]map[string]struct{}
	allowedWildcardURLHosts map[string][]string
}

// URLValidationErrorKind identifies why a URL failed validation.
type URLValidationErrorKind string

const (
	// URLValidationInvalidURL indicates that a value could not be parsed as a URL.
	URLValidationInvalidURL URLValidationErrorKind = "invalid URL"
	// URLValidationHostNotAllowed indicates that a URL host is not permitted.
	URLValidationHostNotAllowed URLValidationErrorKind = "host not allowed"
	// URLValidationSchemeNotAllowed indicates that a URL scheme is not permitted.
	URLValidationSchemeNotAllowed URLValidationErrorKind = "scheme not allowed"
)

// URLValidationError describes a URL validation failure in a SparkApplication field.
type URLValidationError struct {
	Field  string
	Value  string
	Scheme string
	Host   string
	Kind   URLValidationErrorKind
	Err    error
}

func (e *URLValidationError) Error() string {
	switch e.Kind {
	case URLValidationInvalidURL:
		return fmt.Sprintf("%s contains a value that is not a valid URL: %q: %v", e.Field, e.Value, e.Err)
	case URLValidationHostNotAllowed:
		return fmt.Sprintf("%s contains a URL with host %q which is not in the allowed list: %q", e.Field, e.Host, e.Value)
	default:
		return fmt.Sprintf("%s contains a value with URL scheme %q which is not in the allowed list: %q", e.Field, e.Scheme, e.Value)
	}
}

func (e *URLValidationError) Unwrap() error {
	return e.Err
}

// NewSparkApplicationValidator creates a new SparkApplicationValidator instance.
// enableURLSchemeValidation turns on the fetch-field URL-scheme check (default off / opt-in).
// allowedURLSchemes, allowAllURLHostsSchemes, and scheme-qualified allowedURLHosts describe remote
// URL policy for fetch-capable spec fields. A remote URL must use an allowed scheme. Its host must
// match a scheme-qualified exact or wildcard rule unless its scheme is explicitly configured to allow
// all hosts. Local paths, file://, and local:// remain allowed without host configuration.
func NewSparkApplicationValidator(client client.Client, enableResourceQuotaEnforcement bool, enableURLSchemeValidation bool, allowedURLSchemes, allowAllURLHostsSchemes, allowedURLHosts, allowedWildcardURLHosts []string) *SparkApplicationValidator {
	return &SparkApplicationValidator{
		client: client,

		enableResourceQuotaEnforcement: enableResourceQuotaEnforcement,
		enableURLSchemeValidation:      enableURLSchemeValidation,
		allowedURLSchemes:              normalizedURLSchemes(allowedURLSchemes),
		allowAllURLHostsSchemes:        normalizedURLSchemes(allowAllURLHostsSchemes),
		allowedURLHosts:                normalizedURLHosts(allowedURLHosts),
		allowedWildcardURLHosts:        normalizedWildcardURLHosts(allowedWildcardURLHosts),
	}
}

func normalizedURLSchemes(schemes []string) map[string]struct{} {
	allowed := make(map[string]struct{}, len(schemes))
	for _, scheme := range schemes {
		scheme = strings.TrimSpace(strings.ToLower(scheme))
		if scheme != "" {
			allowed[scheme] = struct{}{}
		}
	}
	return allowed
}

func normalizedURLHosts(hosts []string) map[string]map[string]struct{} {
	allowed := make(map[string]map[string]struct{})
	for _, host := range hosts {
		scheme, hostname, _ := parseAllowedURLHost(host, false)
		if allowed[scheme] == nil {
			allowed[scheme] = make(map[string]struct{})
		}
		allowed[scheme][hostname] = struct{}{}
	}
	return allowed
}

func normalizedWildcardURLHosts(hosts []string) map[string][]string {
	allowed := make(map[string][]string)
	for _, host := range hosts {
		scheme, hostname, _ := parseAllowedURLHost(host, true)
		allowed[scheme] = append(allowed[scheme], strings.TrimPrefix(hostname, "*."))
	}
	return allowed
}

func parseAllowedURLHost(value string, wildcard bool) (string, string, error) {
	value = strings.TrimSpace(value)
	u, err := url.Parse(value)
	if err != nil || u.Scheme == "" || u.Host == "" || u.User != nil || u.Port() != "" || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return "", "", fmt.Errorf("invalid allowed URL host %q: use a scheme-qualified authority such as https://example.com", value)
	}
	scheme := strings.ToLower(u.Scheme)
	host := strings.ToLower(u.Hostname())
	if wildcard {
		if !strings.HasPrefix(host, "*.") || len(host) == len("*.") || strings.Contains(host[2:], "*") {
			return "", "", fmt.Errorf("invalid allowed wildcard URL host %q: use a scheme-qualified leftmost wildcard such as https://*.example.com", value)
		}
	} else if strings.Contains(host, "*") {
		return "", "", fmt.Errorf("invalid allowed URL host %q: use --allowed-wildcard-url-hosts for wildcard hosts", value)
	}
	return scheme, host, nil
}

// ValidateAllowedURLHosts rejects unqualified, malformed, or misplaced wildcard host entries
// before webhook startup.
func ValidateAllowedURLHosts(hosts, wildcardHosts []string) error {
	for _, host := range hosts {
		if _, _, err := parseAllowedURLHost(host, false); err != nil {
			return err
		}
	}
	for _, host := range wildcardHosts {
		if _, _, err := parseAllowedURLHost(host, true); err != nil {
			return err
		}
	}
	return nil
}

// ValidateAllowAllURLHostsSchemes requires every host-policy bypass scheme to also be present in
// the remote URL scheme allowlist.
func ValidateAllowAllURLHostsSchemes(allowedSchemes, allowAllHostsSchemes []string) error {
	allowed := normalizedURLSchemes(allowedSchemes)
	for _, scheme := range allowAllHostsSchemes {
		scheme = strings.TrimSpace(strings.ToLower(scheme))
		if scheme == "" {
			return fmt.Errorf("allowed URL scheme with all hosts cannot be empty")
		}
		if _, ok := allowed[scheme]; !ok {
			return fmt.Errorf("allowed URL scheme with all hosts %q must also be listed in --allowed-url-schemes", scheme)
		}
	}
	return nil
}

var _ admission.Validator[*v1beta2.SparkApplication] = &SparkApplicationValidator{}

// ValidateCreate implements admission.Validator.
func (v *SparkApplicationValidator) ValidateCreate(ctx context.Context, app *v1beta2.SparkApplication) (warnings admission.Warnings, err error) {
	if app == nil {
		return nil, nil
	}

	logger := log.FromContext(ctx)
	logger.Info("Validating SparkApplication create", "state", util.GetApplicationState(app))

	// Validate metadata.name early to prevent downstream Service creation failures
	if err := v.validateName(app.Name); err != nil {
		return nil, err
	}
	if err := v.validateSpec(ctx, app); err != nil {
		return nil, err
	}

	if v.enableResourceQuotaEnforcement {
		if err := v.validateResourceUsage(ctx, app); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

// ValidateUpdate implements admission.Validator.
func (v *SparkApplicationValidator) ValidateUpdate(ctx context.Context, oldApp *v1beta2.SparkApplication, newApp *v1beta2.SparkApplication) (warnings admission.Warnings, err error) {
	if oldApp == nil || newApp == nil {
		return nil, nil
	}

	logger := log.FromContext(ctx)
	logger.Info("Validating SparkApplication update", "state", util.GetApplicationState(newApp))

	// Name is immutable in Kubernetes, but validate anyway for safety in case of admission reconcilers
	if err := v.validateName(newApp.Name); err != nil {
		return nil, err
	}

	// Skip validating when spec does not change.
	if equality.Semantic.DeepEqual(oldApp.Spec, newApp.Spec) {
		return nil, nil
	}

	if err := v.validateSpec(ctx, newApp); err != nil {
		return nil, err
	}

	// Validate SparkApplication resource usage when resource quota enforcement is enabled.
	if v.enableResourceQuotaEnforcement {
		if err := v.validateResourceUsage(ctx, newApp); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

// ValidateDelete implements admission.Validator.
func (v *SparkApplicationValidator) ValidateDelete(ctx context.Context, app *v1beta2.SparkApplication) (warnings admission.Warnings, err error) {
	if app == nil {
		return nil, nil
	}

	logger := log.FromContext(ctx)
	logger.Info("Validating SparkApplication delete", "state", util.GetApplicationState(app))
	return nil, nil
}

func (v *SparkApplicationValidator) validateSpec(ctx context.Context, app *v1beta2.SparkApplication) error {
	if err := v.validateSparkVersion(app); err != nil {
		return err
	}

	if app.Spec.NodeSelector != nil && (app.Spec.Driver.NodeSelector != nil || app.Spec.Executor.NodeSelector != nil) {
		return fmt.Errorf("node selector cannot be defined at both SparkApplication and Driver/Executor")
	}

	servicePorts := make(map[int32]bool)
	ingressURLFormats := make(map[string]bool)
	for _, item := range app.Spec.DriverIngressOptions {
		if item.ServicePort == nil {
			return fmt.Errorf("DriverIngressOptions has nill ServicePort")
		}
		if servicePorts[*item.ServicePort] {
			return fmt.Errorf("DriverIngressOptions has duplicate ServicePort: %d", *item.ServicePort)
		}
		servicePorts[*item.ServicePort] = true

		if item.IngressURLFormat == "" {
			return fmt.Errorf("DriverIngressOptions has empty IngressURLFormat")
		}
		if ingressURLFormats[item.IngressURLFormat] {
			return fmt.Errorf("DriverIngressOptions has duplicate IngressURLFormat: %s", item.IngressURLFormat)
		}
		ingressURLFormats[item.IngressURLFormat] = true
	}

	if err := validateSparkConf(app.Spec.SparkConf, app.Namespace); err != nil {
		return err
	}

	if v.enableURLSchemeValidation {
		if err := v.validateURLSchemes(&app.Spec, "spec."); err != nil {
			return err
		}
	}

	return nil
}

// alwaysAllowedURLSchemes are URL schemes that never reach the operator's network when they
// have no authority: they reference files already present on the submitter/driver/executor
// filesystem or baked into the container image, so they are not SSRF vectors and are always
// permitted.
//   - "" (schemeless): a relative or absolute local path.
//   - "file": a file:// URI on the local filesystem.
//   - "local": a local:// URI resolved inside the driver/executor container image.
var alwaysAllowedURLSchemes = []string{"", "file", "local"}

// depsURLFieldsExempt are v1beta2.Dependencies json tags whose values are NOT dereferenced as
// URLs and so are deliberately skipped by the URL-scheme check: they are Maven coordinates
// (groupId:artifactId:version), not fetchable URLs. Every other []string field of Dependencies
// is URL-checked automatically (see depsURLFields), so a newly-added fetch-capable field is
// covered without code changes; a new non-URL field must be added here.
var depsURLFieldsExempt = []string{"packages", "excludePackages"}

// sparkConfURLKeys is the allow-set of sparkConf keys the operator forwards to spark-submit and
// dereferences at submit time, so a remote URL in one of them is fetched by the operator's own
// principal (an SSRF vector). Only these keys are URL-checked; every other sparkConf entry is a
// runtime config consumed by the driver/executor (spark.eventLog.dir, spark.sql.warehouse.dir,
// spark.kubernetes.driverEnv.*, ...) that the operator never fetches, so scheme-checking it would
// wrongly reject legitimate values and couple submit-time policy to runtime config.
//
// This is a positive allow-set rather than a scan-with-deny-list precisely so runtime keys are
// untouched by default. Maven-coordinate keys (spark.jars.packages, spark.jars.excludePackages)
// are deliberately absent because they are not fetchable URLs.
//
// spark.jars / spark.files / spark.submit.pyFiles / spark.archives mirror
// spec.deps.{jars,files,pyFiles,archives}. spark.jars.repositories mirrors
// spec.deps.repositories: spark-submit resolves package dependencies from the operator pod at
// submit time, so repository URLs are fetched with the operator's principal.
//
// spark.kubernetes.file.upload.path is deliberately excluded. It selects an upload destination for
// local artifacts; it is not a source URL that spark-submit retrieves. Destination policy belongs to
// workload egress and artifact-storage controls, not this fetch-source scheme allow-list.
//
// Pod-template keys are included defensively. A driver template can be fetched during submission;
// executor templates are normally fetched later by the driver. The operator currently overwrites
// both user values with local /tmp paths before spark-submit, so user URLs do not reach either
// fetch path. Keep validating them in case that override is removed or bypassed.
var sparkConfURLKeys = map[string]struct{}{
	"spark.jars":              {},
	"spark.files":             {},
	"spark.submit.pyFiles":    {},
	"spark.archives":          {},
	"spark.jars.repositories": {},
	common.SparkKubernetesDriverPodTemplateFile:   {},
	common.SparkKubernetesExecutorPodTemplateFile: {},
}

// depsURLField is one []string field of v1beta2.Dependencies to URL-check: its struct field
// index and the error label derived from its json tag.
type depsURLField struct {
	index int
	label string
}

// depsURLFields is the URL-check plan for v1beta2.Dependencies, computed once from the struct's
// json tags rather than re-reflected on every admission. See depsURLFieldsExempt for exclusions.
var depsURLFields = buildDepsURLFields()

func buildDepsURLFields() []depsURLField {
	var fields []depsURLField
	rt := reflect.TypeFor[v1beta2.Dependencies]()
	stringSlice := reflect.TypeFor[[]string]()
	for i := range rt.NumField() {
		f := rt.Field(i)
		if f.Type != stringSlice {
			continue
		}
		tag := strings.Split(f.Tag.Get("json"), ",")[0]
		if tag == "" || tag == "-" || slices.Contains(depsURLFieldsExempt, tag) {
			continue
		}
		fields = append(fields, depsURLField{index: i, label: "spec.deps." + tag})
	}
	return fields
}

// validateURLSchemes rejects any user-supplied value that is forwarded to the operator's
// spark-submit and dereferenced as a remote URL whose scheme is not in the allowed set.
// spark-submit runs in the operator pod, so an http/https/etc. URL in any of these fields is
// fetched by the operator's principal (its ServiceAccount, mounted secrets, IRSA/Workload
// Identity, VPC reachability) - an SSRF vector.
//
// Only the submit-time sparkConf keys the operator forwards to spark-submit (sparkConfURLKeys)
// are checked; their comma-separated values (spark.jars, spark.files, ...) are split. Runtime
// sparkConf keys are left untouched - see sparkConfURLKeys for why.
//
// spec.hadoopConf is out of scope: its values become spark.hadoop.* config consumed by the
// driver/executor at runtime, not URLs the operator fetches at submit time, and many are
// legitimately host/endpoint-shaped (fs.s3a.endpoint, ...) that a scheme check would wrongly reject.
func (v *SparkApplicationValidator) validateURLSchemes(spec *v1beta2.SparkApplicationSpec, fieldPrefix string) error {
	// Collect every scheme violation rather than returning on the first: a user with several
	// bad URLs should see them all in one admission response instead of fixing them one round-trip
	// at a time. The order below is deterministic (mainApplicationFile, then deps.* in struct-field
	// order, then sparkConf keys sorted) so the same spec always yields the same error text - map
	// iteration order is randomized in Go, so the sparkConf keys must be sorted explicitly.
	var errs []error

	// spec.mainApplicationFile is a single URI forwarded as the final spark-submit argument.
	if spec.MainApplicationFile != nil {
		errs = append(errs, v.checkURLScheme(fieldPrefix+"mainApplicationFile", *spec.MainApplicationFile)...)
	}

	// spec.deps fetch-capable lists (--jars, --files, --py-files, --archives, --repositories).
	errs = append(errs, v.validateDepsURLSchemes(&spec.Deps, fieldPrefix+"deps.")...)

	// spec.sparkConf: only the submit-time keys the operator itself dereferences (sparkConfURLKeys)
	// are checked; runtime keys are left alone. Iterate the allow-set in sorted order so the error
	// text is deterministic regardless of Go's randomized map iteration. Values may be comma-
	// separated URI lists (spark.jars, spark.files, ...) and are split.
	for _, key := range slices.Sorted(maps.Keys(sparkConfURLKeys)) {
		value, ok := spec.SparkConf[key]
		if !ok {
			continue
		}
		field := fmt.Sprintf("%ssparkConf[%q]", fieldPrefix, key)
		errs = append(errs, v.checkURLSchemes(field, strings.Split(value, ","))...)
	}

	return errors.Join(errs...)
}

// validateDepsURLSchemes URL-checks the fetch-capable []string fields of spec.deps, using the
// field plan computed once in depsURLFields. Each value is split on commas because
// dependenciesOption joins these slices into Spark's comma-delimited CLI arguments. The error
// label is the field's json tag, so there is no hand-maintained name-to-field mapping to drift
// from the type. It returns every violation found (see validateURLSchemes) rather than stopping
// at the first.
func (v *SparkApplicationValidator) validateDepsURLSchemes(deps *v1beta2.Dependencies, fieldPrefix string) []error {
	var errs []error
	rv := reflect.ValueOf(deps).Elem()
	for _, f := range depsURLFields {
		values := rv.Field(f.index).Interface().([]string)
		field := fieldPrefix + strings.TrimPrefix(f.label, "spec.deps.")

		for _, value := range values {
			errs = append(errs, v.checkURLSchemes(field, strings.Split(value, ","))...)
		}
	}
	return errs
}

// checkURLSchemes runs checkURLScheme over each value of a field that holds a list of URIs,
// returning every violation rather than stopping at the first.
func (v *SparkApplicationValidator) checkURLSchemes(field string, values []string) []error {
	var errs []error
	for _, value := range values {
		errs = append(errs, v.checkURLScheme(field, value)...)
	}
	return errs
}

// checkURLScheme rejects value unless its URL scheme is always allowed (schemeless, file://,
// local://) or in the operator's configured allow list. It fails closed: anything url.Parse
// can't handle (e.g. embedded control chars, which java.net.URI also rejects) is treated as
// suspect, not waved through. "//host/path" is the one tricky case - it parses with an empty
// scheme but a non-empty host, so guard on Host to tell a real local path (no host) from a
// network-path reference.
//
// It returns a slice of at most one error (empty when the value is allowed) so callers can
// accumulate violations across many fields and surface them together; see validateURLSchemes.
func (v *SparkApplicationValidator) checkURLScheme(field, value string) []error {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	u, err := url.Parse(value)
	if err != nil {
		return []error{&URLValidationError{
			Field: field,
			Value: value,
			Kind:  URLValidationInvalidURL,
			Err:   err,
		}}
	}
	scheme := strings.ToLower(u.Scheme)
	if slices.Contains(alwaysAllowedURLSchemes, scheme) {
		// Any authority makes an otherwise local form ambiguous or remote. In particular,
		// "//host/x", "file://host/x", and "local://host/x" must not use this exemption.
		if u.Host != "" {
			return []error{&URLValidationError{
				Field: field,
				Value: value,
				Host:  u.Host,
				Kind:  URLValidationHostNotAllowed,
			}}
		}
		return nil
	}
	if _, allowed := v.allowedURLSchemes[scheme]; !allowed {
		return []error{&URLValidationError{
			Field:  field,
			Value:  value,
			Scheme: scheme,
			Kind:   URLValidationSchemeNotAllowed,
		}}
	}
	if !v.isAllowedURLHost(scheme, u.Hostname()) {
		return []error{&URLValidationError{
			Field: field,
			Value: value,
			Host:  u.Hostname(),
			Kind:  URLValidationHostNotAllowed,
		}}
	}
	return nil
}

func (v *SparkApplicationValidator) isAllowedURLHost(scheme, host string) bool {
	host = strings.ToLower(host)
	if host == "" {
		return false
	}
	if _, allowed := v.allowAllURLHostsSchemes[scheme]; allowed {
		return true
	}
	if _, allowed := v.allowedURLHosts[scheme][host]; allowed {
		return true
	}
	if net.ParseIP(host) != nil {
		return false
	}
	for _, wildcardHost := range v.allowedWildcardURLHosts[scheme] {
		if strings.HasSuffix(host, "."+wildcardHost) && host != wildcardHost {
			return true
		}
	}
	return false
}

// validateName ensures the SparkApplication metadata.name is a valid DNS-1035 label
// This prevents failures later when creating related resources like Services which
// require DNS-1035 compliant names.
func (v *SparkApplicationValidator) validateName(name string) error {
	if errs := validation.IsDNS1035Label(name); len(errs) > 0 {
		return fmt.Errorf("invalid SparkApplication name %q: %s", name, strings.Join(errs, ", "))
	}
	return nil
}

func (v *SparkApplicationValidator) validateSparkVersion(app *v1beta2.SparkApplication) error {
	// The pod template feature requires Spark version 3.0.0 or higher.
	if app.Spec.Driver.Template != nil || app.Spec.Executor.Template != nil {
		if util.CompareSemanticVersion(app.Spec.SparkVersion, "3.0.0") < 0 {
			return fmt.Errorf("pod template feature requires Spark version 3.0.0 or higher")
		}
	}
	return nil
}

func (v *SparkApplicationValidator) validateResourceUsage(ctx context.Context, app *v1beta2.SparkApplication) error {
	requests, err := getResourceList(app)
	if err != nil {
		return fmt.Errorf("failed to calculate resource quests: %v", err)
	}

	resourceQuotaList := &corev1.ResourceQuotaList{}
	if err := v.client.List(ctx, resourceQuotaList, client.InNamespace(app.Namespace)); err != nil {
		return fmt.Errorf("failed to list resource quotas: %v", err)
	}

	for _, resourceQuota := range resourceQuotaList.Items {
		// Scope selectors not currently supported, ignore any ResourceQuota that does not match everything.
		// TODO: Add support for scope selectors.
		if resourceQuota.Spec.ScopeSelector != nil || len(resourceQuota.Spec.Scopes) > 0 {
			continue
		}

		if !validateResourceQuota(requests, resourceQuota) {
			return fmt.Errorf("failed to validate resource quota \"%s/%s\"", resourceQuota.Namespace, resourceQuota.Name)
		}
	}

	return nil
}
