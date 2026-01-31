module github.com/kubeflow/spark-operator/v2

go 1.24.10

tool k8s.io/code-generator

require (
	github.com/go-logr/logr v1.4.3
	github.com/golang/glog v1.2.5
	github.com/google/uuid v1.6.0
	github.com/onsi/ginkgo/v2 v2.27.2
	github.com/onsi/gomega v1.38.2
	github.com/prometheus/client_golang v1.23.2
	github.com/robfig/cron/v3 v3.0.1
	github.com/spf13/cobra v1.10.1
	github.com/spf13/viper v1.21.0
	github.com/stretchr/testify v1.11.1
	go.uber.org/zap v1.27.0
	golang.org/x/mod v0.29.0
	golang.org/x/time v0.14.0
	helm.sh/helm/v3 v3.18.5
	k8s.io/api v0.33.3
	k8s.io/apiextensions-apiserver v0.33.3
	k8s.io/apimachinery v0.33.3
	k8s.io/apiserver v0.33.3
	k8s.io/client-go v0.33.3
	k8s.io/component-base v0.33.3
	k8s.io/klog/v2 v2.130.1
	k8s.io/kube-openapi v0.0.0-20250318190949-c8a335a9a2ff
	k8s.io/utils v0.0.0-20241104100929-3ea5e8cea738
	sigs.k8s.io/controller-runtime v0.20.4
	sigs.k8s.io/scheduler-plugins v0.32.7
	sigs.k8s.io/yaml v1.6.0
	volcano.sh/apis v1.10.0
)

require (
	dario.cat/mergo v1.0.1 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20250102033503-faa5f7b0171c // indirect
	github.com/BurntSushi/toml v1.5.0 // indirect
	github.com/MakeNowJust/heredoc v1.0.0 // indirect
	github.com/Masterminds/goutils v1.1.1 // indirect
	github.com/Masterminds/semver/v3 v3.4.0 // indirect
	github.com/Masterminds/sprig/v3 v3.3.0 // indirect
	github.com/Masterminds/squirrel v1.5.4 // indirect
	github.com/asaskevich/govalidator v0.0.0-20230301143203-a9d515a09cc2 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/blang/semver/v4 v4.0.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/chai2010/gettext-go v1.0.3 // indirect
	github.com/containerd/containerd v1.7.29 // indirect
	github.com/containerd/errdefs v0.3.0 // indirect
	github.com/containerd/log v0.1.0 // indirect
	github.com/containerd/platforms v0.2.1 // indirect
	github.com/cyphar/filepath-securejoin v0.4.1 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/emicklei/go-restful/v3 v3.12.1 // indirect
	github.com/evanphx/json-patch v5.9.11+incompatible // indirect
	github.com/evanphx/json-patch/v5 v5.9.11 // indirect
	github.com/exponent-io/jsonpath v0.0.0-20210407135951-1de76d718b3f // indirect
	github.com/fatih/color v1.17.0 // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/fxamacker/cbor/v2 v2.7.0 // indirect
	github.com/go-errors/errors v1.5.1 // indirect
	github.com/go-gorp/gorp/v3 v3.1.0 // indirect
	github.com/go-logr/zapr v1.3.0 // indirect
	github.com/go-openapi/jsonpointer v0.21.0 // indirect
	github.com/go-openapi/jsonreference v0.21.0 // indirect
	github.com/go-openapi/swag v0.23.0 // indirect
	github.com/go-task/slim-sprig/v3 v3.0.0 // indirect
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/btree v1.1.3 // indirect
	github.com/google/gnostic-models v0.6.9 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/google/pprof v0.0.0-20250403155104-27863c87afa6 // indirect
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510 // indirect
	github.com/gorilla/websocket v1.5.4-0.20250319132907-e064f32e3674 // indirect
	github.com/gosuri/uitable v0.0.4 // indirect
	github.com/gregjones/httpcache v0.0.0-20190611155906-901d90724c79 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/huandu/xstrings v1.5.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jmoiron/sqlx v1.4.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/lann/builder v0.0.0-20180802200727-47ae307949d0 // indirect
	github.com/lann/ps v0.0.0-20150810152359-62de8c46ede0 // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/liggitt/tabwriter v0.0.0-20181228230101-89fcab3d43de // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-runewidth v0.0.15 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/go-wordwrap v1.0.1 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/moby/spdystream v0.5.0 // indirect
	github.com/moby/term v0.5.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/monochromegane/go-gitignore v0.0.0-20200626010858-205db1a8cc00 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/mxk/go-flowrate v0.0.0-20140419014527-cca7078d478f // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.1 // indirect
	github.com/pelletier/go-toml/v2 v2.2.4 // indirect
	github.com/peterbourgon/diskv v2.0.1+incompatible // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/rubenv/sql-migrate v1.8.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/sagikazarmark/locafero v0.11.0 // indirect
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.2 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/sourcegraph/conc v0.3.1-0.20240121214520-5f936abd7ae8 // indirect
	github.com/spf13/afero v1.15.0 // indirect
	github.com/spf13/cast v1.10.0 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	github.com/xlab/treeprint v1.2.0 // indirect
	go.opentelemetry.io/otel v1.33.0 // indirect
	go.opentelemetry.io/otel/trace v1.33.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.yaml.in/yaml/v2 v2.4.2 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/crypto v0.45.0 // indirect
	golang.org/x/net v0.47.0 // indirect
	golang.org/x/oauth2 v0.30.0 // indirect
	golang.org/x/sync v0.18.0 // indirect
	golang.org/x/sys v0.38.0 // indirect
	golang.org/x/term v0.37.0 // indirect
	golang.org/x/text v0.31.0 // indirect
	golang.org/x/tools v0.38.0 // indirect
	golang.org/x/tools/go/packages/packagestest v0.1.1-deprecated // indirect
	gomodules.xyz/jsonpatch/v2 v2.4.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250102185135-69823020774d // indirect
	google.golang.org/grpc v1.69.4 // indirect
	google.golang.org/protobuf v1.36.8 // indirect
	gopkg.in/evanphx/json-patch.v4 v4.12.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/cli-runtime v0.33.3 // indirect
	k8s.io/code-generator v0.32.7 // indirect
	k8s.io/gengo/v2 v2.0.0-20240911193312-2b36238f13e9 // indirect
	k8s.io/kubectl v0.33.3 // indirect
	oras.land/oras-go/v2 v2.6.0 // indirect
	sigs.k8s.io/json v0.0.0-20241010143419-9aa6b5e7a4b3 // indirect
	sigs.k8s.io/kustomize/api v0.19.0 // indirect
	sigs.k8s.io/kustomize/kyaml v0.19.0 // indirect
	sigs.k8s.io/randfill v1.0.0 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.6.0 // indirect
)

replace (
	k8s.io/api => k8s.io/api v0.32.5
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.32.5
	k8s.io/apimachinery => k8s.io/apimachinery v0.32.5
	k8s.io/apiserver => k8s.io/apiserver v0.32.5
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.32.5
	k8s.io/client-go => k8s.io/client-go v0.32.5
	k8s.io/cloud-provider => k8s.io/cloud-provider v0.32.5
	k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.32.5
	k8s.io/code-generator => k8s.io/code-generator v0.32.5
	k8s.io/component-base => k8s.io/component-base v0.32.5
	k8s.io/component-helpers => k8s.io/component-helpers v0.32.5
	k8s.io/componenti-base => k8s.io/componenti-base v0.32.5
	k8s.io/controller-manager => k8s.io/controller-manager v0.32.5
	k8s.io/cri-api => k8s.io/cri-api v0.32.5
	k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.32.5
	k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.32.5
	k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.32.5
	k8s.io/kube-proxy => k8s.io/kube-proxy v0.32.5
	k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.32.5
	k8s.io/kubectl => k8s.io/kubectl v0.32.5
	k8s.io/kubelet => k8s.io/kubelet v0.32.5
	k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.32.5
	k8s.io/metrics => k8s.io/metrics v0.32.5
	k8s.io/mount-utils => k8s.io/mount-utils v0.32.5
	k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.32.5
	k8s.io/sample-cli-plugin => k8s.io/sample-cli-plugin v0.32.5
	k8s.io/sample-controller => k8s.io/sample-controller v0.32.5
)
