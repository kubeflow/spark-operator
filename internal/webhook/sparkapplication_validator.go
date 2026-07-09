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
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:admissionReviewVersions=v1,failurePolicy=fail,groups=sparkoperator.k8s.io,matchPolicy=Exact,mutating=false,name=validate-sparkapplication.sparkoperator.k8s.io,path=/validate-sparkoperator-k8s-io-v1beta2-sparkapplication,reinvocationPolicy=Never,resources=sparkapplications,sideEffects=None,verbs=create;update,versions=v1beta2,webhookVersions=v1

type SparkApplicationValidator struct {
	client client.Client

	enableResourceQuotaEnforcement bool
	// allowedURLSchemes is the set of additional URL schemes permitted in fetch-capable spec
	// fields (sparkConf values, deps.*, mainApplicationFile) beyond the always-allowed local
	// schemes (schemeless, file://, local://). Any value whose URL scheme is not always-allowed
	// and not in this set is rejected at admission time. Add schemes your workloads require
	// (e.g. gs, s3a, hdfs).
	allowedURLSchemes map[string]struct{}
}

// NewSparkApplicationValidator creates a new SparkApplicationValidator instance.
// allowedURLSchemes lists URL schemes permitted in fetch-capable spec fields (sparkConf values,
// deps.*, mainApplicationFile) in addition to the always-allowed local schemes (schemeless,
// file://, local://). Any other scheme is rejected.
func NewSparkApplicationValidator(client client.Client, enableResourceQuotaEnforcement bool, allowedURLSchemes []string) *SparkApplicationValidator {
	allowed := make(map[string]struct{}, len(allowedURLSchemes))
	for _, s := range allowedURLSchemes {
		s = strings.TrimSpace(strings.ToLower(s))
		if s != "" {
			allowed[s] = struct{}{}
		}
	}
	return &SparkApplicationValidator{
		client: client,

		enableResourceQuotaEnforcement: enableResourceQuotaEnforcement,
		allowedURLSchemes:              allowed,
	}
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

	if err := v.validateURLSchemes(app); err != nil {
		return err
	}

	return nil
}

// alwaysAllowedURLSchemes are URL schemes that never reach the operator's network: they
// reference files already present on the submitter/driver/executor filesystem or baked into
// the container image, so they are not SSRF vectors and are always permitted.
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

// sparkConfKeysExempt are sparkConf keys whose comma-separated values are NOT dereferenced as
// URLs: they hold Maven coordinates (groupId:artifactId:version) or Ivy XML paths resolved by
// the Spark Ivy subsystem, not operator-fetched URLs. spark.jars.packages is the primary case:
// a value like "org.postgresql:postgresql:42.7.3" parses as scheme "org.postgresql" but is not
// a URL. spark.jars.excludePackages and spark.jars.ivySettings follow the same pattern.
var sparkConfKeysExempt = map[string]struct{}{
	"spark.jars.packages":        {},
	"spark.jars.excludePackages": {},
	"spark.jars.ivySettings":     {},
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
// Comma-separated sparkConf values (spark.jars, spark.files, ...) are split.
//
// spec.hadoopConf is out of scope: its values become spark.hadoop.* config consumed by the
// driver/executor at runtime, not URLs the operator fetches at submit time, and many are
// legitimately host/endpoint-shaped (fs.s3a.endpoint, ...) that a scheme check would wrongly reject.
func (v *SparkApplicationValidator) validateURLSchemes(app *v1beta2.SparkApplication) error {
	// Collect every scheme violation rather than returning on the first: a user with several
	// bad URLs should see them all in one admission response instead of fixing them one round-trip
	// at a time. The order below is deterministic (mainApplicationFile, then deps.* in struct-field
	// order, then sparkConf keys sorted) so the same spec always yields the same error text - map
	// iteration order is randomized in Go, so the sparkConf keys must be sorted explicitly.
	var errs []error

	// spec.mainApplicationFile is a single URI forwarded as the final spark-submit argument.
	if app.Spec.MainApplicationFile != nil {
		errs = append(errs, v.checkURLScheme("spec.mainApplicationFile", *app.Spec.MainApplicationFile)...)
	}

	// spec.deps fetch-capable lists (--jars, --files, --py-files, --archives, --repositories).
	errs = append(errs, v.validateDepsURLSchemes(&app.Spec.Deps)...)

	// spec.sparkConf values; some conf keys carry comma-separated URI lists, but some
	// (spark.jars.packages, spark.jars.excludePackages, spark.jars.ivySettings) hold Maven
	// coordinates or Ivy XML paths that parse as invalid URL schemes and are exempt.
	for _, key := range slices.Sorted(maps.Keys(app.Spec.SparkConf)) {
		if _, exempt := sparkConfKeysExempt[key]; exempt {
			continue
		}
		field := fmt.Sprintf("spec.sparkConf[%q]", key)
		errs = append(errs, v.checkURLSchemes(field, strings.Split(app.Spec.SparkConf[key], ","))...)
	}

	return errors.Join(errs...)
}

// validateDepsURLSchemes URL-checks the fetch-capable []string fields of spec.deps, using the
// field plan computed once in depsURLFields. The error label is the field's json tag, so there
// is no hand-maintained name-to-field mapping to drift from the type. It returns every violation
// found (see validateURLSchemes) rather than stopping at the first.
func (v *SparkApplicationValidator) validateDepsURLSchemes(deps *v1beta2.Dependencies) []error {
	var errs []error
	rv := reflect.ValueOf(deps).Elem()
	for _, f := range depsURLFields {
		values := rv.Field(f.index).Interface().([]string)
		errs = append(errs, v.checkURLSchemes(f.label, values)...)
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
		return []error{fmt.Errorf("%s contains a value that is not a valid URL: %q: %w", field, value, err)}
	}
	scheme := strings.ToLower(u.Scheme)
	if slices.Contains(alwaysAllowedURLSchemes, scheme) {
		// "//host/x" has an empty scheme but a host - it's a network path, not a local one,
		// so don't let the empty-scheme exemption cover it.
		if scheme == "" && u.Host != "" {
			return []error{fmt.Errorf("%s contains a scheme-relative URL with host %q which is not a local path: %q", field, u.Host, value)}
		}
		return nil
	}
	if _, allowed := v.allowedURLSchemes[scheme]; !allowed {
		return []error{fmt.Errorf("%s contains a value with URL scheme %q which is not in the allowed list: %q", field, scheme, value)}
	}
	return nil
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
