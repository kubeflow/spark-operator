<p>Packages:</p>
<ul>
<li>
<a href="#sparkoperator.k8s.io%2fv1beta2">sparkoperator.k8s.io/v1beta2</a>
</li>
</ul>
<h2 id="sparkoperator.k8s.io/v1beta2">sparkoperator.k8s.io/v1beta2</h2>
<p>
<p>Package v1beta2 is the v1beta2 version of the API.</p>
</p>
Resource Types:
<ul><li>
<a href="#sparkoperator.k8s.io/v1beta2.ScheduledSparkApplication">ScheduledSparkApplication</a>
</li><li>
<a href="#sparkoperator.k8s.io/v1beta2.SparkApplication">SparkApplication</a>
</li></ul>
<h3 id="sparkoperator.k8s.io/v1beta2.ScheduledSparkApplication">ScheduledSparkApplication
</h3>
<p>
</p>
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>apiVersion</code></br>
string</td>
<td>
<code>
sparkoperator.k8s.io/v1beta2
</code>
</td>
</tr>
<tr>
<td>
<code>kind</code></br>
string
</td>
<td><code>ScheduledSparkApplication</code></td>
</tr>
<tr>
<td>
<code>metadata</code></br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#objectmeta-v1-meta">
Kubernetes meta/v1.ObjectMeta
</a>
</em>
</td>
<td>
Refer to the Kubernetes API documentation for the fields of the
<code>metadata</code> field.
</td>
</tr>
<tr>
<td>
<code>spec</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.ScheduledSparkApplicationSpec">
ScheduledSparkApplicationSpec
</a>
</em>
</td>
<td>
<br/>
<br/>
<table>
<tr>
<td>
<code>schedule</code></br>
<em>
string
</em>
</td>
<td>
<p>Schedule is a cron schedule on which the application should run.</p>
</td>
</tr>
<tr>
<td>
<code>template</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.SparkApplicationSpec">
SparkApplicationSpec
</a>
</em>
</td>
<td>
<p>Template is a template from which SparkApplication instances can be created.</p>
</td>
</tr>
<tr>
<td>
<code>suspend</code></br>
<em>
bool
</em>
</td>
<td>
<em>(Optional)</em>
<p>Suspend is a flag telling the controller to suspend subsequent runs of the application if set to true.
Defaults to false.</p>
</td>
</tr>
<tr>
<td>
<code>concurrencyPolicy</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.ConcurrencyPolicy">
ConcurrencyPolicy
</a>
</em>
</td>
<td>
<p>ConcurrencyPolicy is the policy governing concurrent SparkApplication runs.</p>
</td>
</tr>
<tr>
<td>
<code>successfulRunHistoryLimit</code></br>
<em>
int32
</em>
</td>
<td>
<em>(Optional)</em>
<p>SuccessfulRunHistoryLimit is the number of past successful runs of the application to keep.
Defaults to 1.</p>
</td>
</tr>
<tr>
<td>
<code>failedRunHistoryLimit</code></br>
<em>
int32
</em>
</td>
<td>
<em>(Optional)</em>
<p>FailedRunHistoryLimit is the number of past failed runs of the application to keep.
Defaults to 1.</p>
</td>
</tr>
</table>
</td>
</tr>
<tr>
<td>
<code>status</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.ScheduledSparkApplicationStatus">
ScheduledSparkApplicationStatus
</a>
</em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="sparkoperator.k8s.io/v1beta2.SparkApplication">SparkApplication
</h3>
<p>
<p>SparkApplication represents a Spark application running on and using Kubernetes as a cluster manager.</p>
</p>
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>apiVersion</code></br>
string</td>
<td>
<code>
sparkoperator.k8s.io/v1beta2
</code>
</td>
</tr>
<tr>
<td>
<code>kind</code></br>
string
</td>
<td><code>SparkApplication</code></td>
</tr>
<tr>
<td>
<code>metadata</code></br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#objectmeta-v1-meta">
Kubernetes meta/v1.ObjectMeta
</a>
</em>
</td>
<td>
Refer to the Kubernetes API documentation for the fields of the
<code>metadata</code> field.
</td>
</tr>
<tr>
<td>
<code>spec</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.SparkApplicationSpec">
SparkApplicationSpec
</a>
</em>
</td>
<td>
<br/>
<br/>
<table>
<tr>
<td>
<code>type</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.SparkApplicationType">
SparkApplicationType
</a>
</em>
</td>
<td>
<p>Type tells the type of the Spark application.</p>
</td>
</tr>
<tr>
<td>
<code>sparkVersion</code></br>
<em>
string
</em>
</td>
<td>
<p>SparkVersion is the version of Spark the application uses.</p>
</td>
</tr>
<tr>
<td>
<code>mode</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.DeployMode">
DeployMode
</a>
</em>
</td>
<td>
<p>Mode is the deployment mode of the Spark application.</p>
</td>
</tr>
<tr>
<td>
<code>image</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>Image is the container image for the driver, executor, and init-container. Any custom container images for the
driver, executor, or init-container takes precedence over this.</p>
</td>
</tr>
<tr>
<td>
<code>initContainerImage</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>InitContainerImage is the image of the init-container to use. Overrides Spec.Image if set.</p>
</td>
</tr>
<tr>
<td>
<code>imagePullPolicy</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>ImagePullPolicy is the image pull policy for the driver, executor, and init-container.</p>
</td>
</tr>
<tr>
<td>
<code>imagePullSecrets</code></br>
<em>
[]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>ImagePullSecrets is the list of image-pull secrets.</p>
</td>
</tr>
<tr>
<td>
<code>mainClass</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>MainClass is the fully-qualified main class of the Spark application.
This only applies to Java/Scala Spark applications.</p>
</td>
</tr>
<tr>
<td>
<code>mainApplicationFile</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>MainFile is the path to a bundled JAR, Python, or R file of the application.</p>
</td>
</tr>
<tr>
<td>
<code>arguments</code></br>
<em>
[]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>Arguments is a list of arguments to be passed to the application.</p>
</td>
</tr>
<tr>
<td>
<code>sparkConf</code></br>
<em>
map[string]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>SparkConf carries user-specified Spark configuration properties as they would use the  &ldquo;&ndash;conf&rdquo; option in
spark-submit.</p>
</td>
</tr>
<tr>
<td>
<code>hadoopConf</code></br>
<em>
map[string]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>HadoopConf carries user-specified Hadoop configuration properties as they would use the  the &ldquo;&ndash;conf&rdquo; option
in spark-submit.  The SparkApplication controller automatically adds prefix &ldquo;spark.hadoop.&rdquo; to Hadoop
configuration properties.</p>
</td>
</tr>
<tr>
<td>
<code>sparkConfigMap</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>SparkConfigMap carries the name of the ConfigMap containing Spark configuration files such as log4j.properties.
The controller will add environment variable SPARK_CONF_DIR to the path where the ConfigMap is mounted to.</p>
</td>
</tr>
<tr>
<td>
<code>hadoopConfigMap</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>HadoopConfigMap carries the name of the ConfigMap containing Hadoop configuration files such as core-site.xml.
The controller will add environment variable HADOOP_CONF_DIR to the path where the ConfigMap is mounted to.</p>
</td>
</tr>
<tr>
<td>
<code>volumes</code></br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#volume-v1-core">
[]Kubernetes core/v1.Volume
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Volumes is the list of Kubernetes volumes that can be mounted by the driver and/or executors.</p>
</td>
</tr>
<tr>
<td>
<code>driver</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.DriverSpec">
DriverSpec
</a>
</em>
</td>
<td>
<p>Driver is the driver specification.</p>
</td>
</tr>
<tr>
<td>
<code>executor</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.ExecutorSpec">
ExecutorSpec
</a>
</em>
</td>
<td>
<p>Executor is the executor specification.</p>
</td>
</tr>
<tr>
<td>
<code>deps</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.Dependencies">
Dependencies
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Deps captures all possible types of dependencies of a Spark application.</p>
</td>
</tr>
<tr>
<td>
<code>restartPolicy</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.RestartPolicy">
RestartPolicy
</a>
</em>
</td>
<td>
<p>RestartPolicy defines the policy on if and in which conditions the controller should restart an application.</p>
</td>
</tr>
<tr>
<td>
<code>nodeSelector</code></br>
<em>
map[string]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>NodeSelector is the Kubernetes node selector to be added to the driver and executor pods.
This field is mutually exclusive with nodeSelector at podSpec level (driver or executor).
This field will be deprecated in future versions (at SparkApplicationSpec level).</p>
</td>
</tr>
<tr>
<td>
<code>failureRetries</code></br>
<em>
int32
</em>
</td>
<td>
<em>(Optional)</em>
<p>FailureRetries is the number of times to retry a failed application before giving up.
This is best effort and actual retry attempts can be &gt;= the value specified.</p>
</td>
</tr>
<tr>
<td>
<code>retryInterval</code></br>
<em>
int64
</em>
</td>
<td>
<em>(Optional)</em>
<p>RetryInterval is the unit of intervals in seconds between submission retries.</p>
</td>
</tr>
<tr>
<td>
<code>pythonVersion</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>This sets the major Python version of the docker
image used to run the driver and executor containers. Can either be 2 or 3, default 2.</p>
</td>
</tr>
<tr>
<td>
<code>memoryOverheadFactor</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>This sets the Memory Overhead Factor that will allocate memory to non-JVM memory.
For JVM-based jobs this value will default to 0.10, for non-JVM jobs 0.40. Value of this field will
be overridden by <code>Spec.Driver.MemoryOverhead</code> and <code>Spec.Executor.MemoryOverhead</code> if they are set.</p>
</td>
</tr>
<tr>
<td>
<code>monitoring</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.MonitoringSpec">
MonitoringSpec
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Monitoring configures how monitoring is handled.</p>
</td>
</tr>
<tr>
<td>
<code>batchScheduler</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>BatchScheduler configures which batch scheduler will be used for scheduling</p>
</td>
</tr>
<tr>
<td>
<code>timeToLiveSeconds</code></br>
<em>
int64
</em>
</td>
<td>
<em>(Optional)</em>
<p>TimeToLiveSeconds defines the Time-To-Live (TTL) duration in seconds for this SparkAplication
after its termination.
The SparkApplication object will be garbage collected if the current time is more than the
TimeToLiveSeconds since its termination.</p>
</td>
</tr>
<tr>
<td>
<code>batchSchedulerOptions</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.BatchSchedulerConfiguration">
BatchSchedulerConfiguration
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>BatchSchedulerOptions provides fine-grained control on how to batch scheduling.</p>
</td>
</tr>
</table>
</td>
</tr>
<tr>
<td>
<code>status</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.SparkApplicationStatus">
SparkApplicationStatus
</a>
</em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="sparkoperator.k8s.io/v1beta2.ApplicationState">ApplicationState
</h3>
<p>
(<em>Appears on:</em>
<a href="#sparkoperator.k8s.io/v1beta2.SparkApplicationStatus">SparkApplicationStatus</a>)
</p>
<p>
<p>ApplicationState tells the current state of the application and an error message in case of failures.</p>
</p>
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>state</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.ApplicationStateType">
ApplicationStateType
</a>
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>errorMessage</code></br>
<em>
string
</em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="sparkoperator.k8s.io/v1beta2.ApplicationStateType">ApplicationStateType
(<code>string</code> alias)</p></h3>
<p>
(<em>Appears on:</em>
<a href="#sparkoperator.k8s.io/v1beta2.ApplicationState">ApplicationState</a>)
</p>
<p>
<p>ApplicationStateType represents the type of the current state of an application.</p>
</p>
<h3 id="sparkoperator.k8s.io/v1beta2.BatchSchedulerConfiguration">BatchSchedulerConfiguration
</h3>
<p>
(<em>Appears on:</em>
<a href="#sparkoperator.k8s.io/v1beta2.SparkApplicationSpec">SparkApplicationSpec</a>)
</p>
<p>
<p>BatchSchedulerConfiguration used to configure how to batch scheduling Spark Application</p>
</p>
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>queue</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>Queue stands for the resource queue which the application belongs to, it&rsquo;s being used in Volcano batch scheduler.</p>
</td>
</tr>
<tr>
<td>
<code>priorityClassName</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>PriorityClassName stands for the name of k8s PriorityClass resource, it&rsquo;s being used in Volcano batch scheduler.</p>
</td>
</tr>
</tbody>
</table>
<h3 id="sparkoperator.k8s.io/v1beta2.ConcurrencyPolicy">ConcurrencyPolicy
(<code>string</code> alias)</p></h3>
<p>
(<em>Appears on:</em>
<a href="#sparkoperator.k8s.io/v1beta2.ScheduledSparkApplicationSpec">ScheduledSparkApplicationSpec</a>)
</p>
<p>
</p>
<h3 id="sparkoperator.k8s.io/v1beta2.Dependencies">Dependencies
</h3>
<p>
(<em>Appears on:</em>
<a href="#sparkoperator.k8s.io/v1beta2.SparkApplicationSpec">SparkApplicationSpec</a>)
</p>
<p>
<p>Dependencies specifies all possible types of dependencies of a Spark application.</p>
</p>
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>jars</code></br>
<em>
[]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>Jars is a list of JAR files the Spark application depends on.</p>
</td>
</tr>
<tr>
<td>
<code>files</code></br>
<em>
[]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>Files is a list of files the Spark application depends on.</p>
</td>
</tr>
<tr>
<td>
<code>pyFiles</code></br>
<em>
[]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>PyFiles is a list of Python files the Spark application depends on.</p>
</td>
</tr>
<tr>
<td>
<code>jarsDownloadDir</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>JarsDownloadDir is the location to download jars to in the driver and executors.</p>
</td>
</tr>
<tr>
<td>
<code>filesDownloadDir</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>FilesDownloadDir is the location to download files to in the driver and executors.</p>
</td>
</tr>
<tr>
<td>
<code>downloadTimeout</code></br>
<em>
int32
</em>
</td>
<td>
<em>(Optional)</em>
<p>DownloadTimeout specifies the timeout in seconds before aborting the attempt to download
and unpack dependencies from remote locations into the driver and executor pods.</p>
</td>
</tr>
<tr>
<td>
<code>maxSimultaneousDownloads</code></br>
<em>
int32
</em>
</td>
<td>
<em>(Optional)</em>
<p>MaxSimultaneousDownloads specifies the maximum number of remote dependencies to download
simultaneously in a driver or executor pod.</p>
</td>
</tr>
</tbody>
</table>
<h3 id="sparkoperator.k8s.io/v1beta2.DeployMode">DeployMode
(<code>string</code> alias)</p></h3>
<p>
(<em>Appears on:</em>
<a href="#sparkoperator.k8s.io/v1beta2.SparkApplicationSpec">SparkApplicationSpec</a>)
</p>
<p>
<p>DeployMode describes the type of deployment of a Spark application.</p>
</p>
<h3 id="sparkoperator.k8s.io/v1beta2.DriverInfo">DriverInfo
</h3>
<p>
(<em>Appears on:</em>
<a href="#sparkoperator.k8s.io/v1beta2.SparkApplicationStatus">SparkApplicationStatus</a>)
</p>
<p>
<p>DriverInfo captures information about the driver.</p>
</p>
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>webUIServiceName</code></br>
<em>
string
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>webUIPort</code></br>
<em>
int32
</em>
</td>
<td>
<p>UI Details for the UI created via ClusterIP service accessible from within the cluster.</p>
</td>
</tr>
<tr>
<td>
<code>webUIAddress</code></br>
<em>
string
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>webUIIngressName</code></br>
<em>
string
</em>
</td>
<td>
<p>Ingress Details if an ingress for the UI was created.</p>
</td>
</tr>
<tr>
<td>
<code>webUIIngressAddress</code></br>
<em>
string
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>podName</code></br>
<em>
string
</em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="sparkoperator.k8s.io/v1beta2.DriverSpec">DriverSpec
</h3>
<p>
(<em>Appears on:</em>
<a href="#sparkoperator.k8s.io/v1beta2.SparkApplicationSpec">SparkApplicationSpec</a>)
</p>
<p>
<p>DriverSpec is specification of the driver.</p>
</p>
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>SparkPodSpec</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.SparkPodSpec">
SparkPodSpec
</a>
</em>
</td>
<td>
<p>
(Members of <code>SparkPodSpec</code> are embedded into this type.)
</p>
</td>
</tr>
<tr>
<td>
<code>podName</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>PodName is the name of the driver pod that the user creates. This is used for the
in-cluster client mode in which the user creates a client pod where the driver of
the user application runs. It&rsquo;s an error to set this field if Mode is not
in-cluster-client.</p>
</td>
</tr>
<tr>
<td>
<code>serviceAccount</code></br>
<em>
string
</em>
</td>
<td>
<p>ServiceAccount is the name of the Kubernetes service account used by the driver pod
when requesting executor pods from the API server.</p>
</td>
</tr>
<tr>
<td>
<code>javaOptions</code></br>
<em>
string
</em>
</td>
<td>
<p>JavaOptions is a string of extra JVM options to pass to the driver. For instance,
GC settings or other logging.</p>
</td>
</tr>
</tbody>
</table>
<h3 id="sparkoperator.k8s.io/v1beta2.ExecutorSpec">ExecutorSpec
</h3>
<p>
(<em>Appears on:</em>
<a href="#sparkoperator.k8s.io/v1beta2.SparkApplicationSpec">SparkApplicationSpec</a>)
</p>
<p>
<p>ExecutorSpec is specification of the executor.</p>
</p>
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>SparkPodSpec</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.SparkPodSpec">
SparkPodSpec
</a>
</em>
</td>
<td>
<p>
(Members of <code>SparkPodSpec</code> are embedded into this type.)
</p>
</td>
</tr>
<tr>
<td>
<code>instances</code></br>
<em>
int32
</em>
</td>
<td>
<em>(Optional)</em>
<p>Instances is the number of executor instances.</p>
</td>
</tr>
<tr>
<td>
<code>coreRequest</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>CoreRequest is the physical CPU core request for the executors.</p>
</td>
</tr>
<tr>
<td>
<code>javaOptions</code></br>
<em>
string
</em>
</td>
<td>
<p>JavaOptions is a string of extra JVM options to pass to the executors. For instance,
GC settings or other logging.</p>
</td>
</tr>
<tr>
<td>
<code>deleteOnTermination</code></br>
<em>
bool
</em>
</td>
<td>
<p>DeleteOnTermination specify whether executor pods should be deleted in case of failure or normal termination
Optional</p>
</td>
</tr>
</tbody>
</table>
<h3 id="sparkoperator.k8s.io/v1beta2.ExecutorState">ExecutorState
(<code>string</code> alias)</p></h3>
<p>
(<em>Appears on:</em>
<a href="#sparkoperator.k8s.io/v1beta2.SparkApplicationStatus">SparkApplicationStatus</a>)
</p>
<p>
<p>ExecutorState tells the current state of an executor.</p>
</p>
<h3 id="sparkoperator.k8s.io/v1beta2.GPUSpec">GPUSpec
</h3>
<p>
(<em>Appears on:</em>
<a href="#sparkoperator.k8s.io/v1beta2.SparkPodSpec">SparkPodSpec</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>name</code></br>
<em>
string
</em>
</td>
<td>
<p>Name is GPU resource name, such as: nvidia.com/gpu or amd.com/gpu</p>
</td>
</tr>
<tr>
<td>
<code>quantity</code></br>
<em>
int64
</em>
</td>
<td>
<p>Quantity is the number of GPUs to request for driver or executor.</p>
</td>
</tr>
</tbody>
</table>
<h3 id="sparkoperator.k8s.io/v1beta2.MonitoringSpec">MonitoringSpec
</h3>
<p>
(<em>Appears on:</em>
<a href="#sparkoperator.k8s.io/v1beta2.SparkApplicationSpec">SparkApplicationSpec</a>)
</p>
<p>
<p>MonitoringSpec defines the monitoring specification.</p>
</p>
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>exposeDriverMetrics</code></br>
<em>
bool
</em>
</td>
<td>
<p>ExposeDriverMetrics specifies whether to expose metrics on the driver.</p>
</td>
</tr>
<tr>
<td>
<code>exposeExecutorMetrics</code></br>
<em>
bool
</em>
</td>
<td>
<p>ExposeExecutorMetrics specifies whether to expose metrics on the executors.</p>
</td>
</tr>
<tr>
<td>
<code>metricsProperties</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>MetricsProperties is the content of a custom metrics.properties for configuring the Spark metric system.
If not specified, the content in spark-docker/conf/metrics.properties will be used.</p>
</td>
</tr>
<tr>
<td>
<code>prometheus</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.PrometheusSpec">
PrometheusSpec
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Prometheus is for configuring the Prometheus JMX exporter.</p>
</td>
</tr>
</tbody>
</table>
<h3 id="sparkoperator.k8s.io/v1beta2.NameKey">NameKey
</h3>
<p>
(<em>Appears on:</em>
<a href="#sparkoperator.k8s.io/v1beta2.SparkPodSpec">SparkPodSpec</a>)
</p>
<p>
<p>NameKey represents the name and key of a SecretKeyRef.</p>
</p>
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>name</code></br>
<em>
string
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>key</code></br>
<em>
string
</em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="sparkoperator.k8s.io/v1beta2.NamePath">NamePath
</h3>
<p>
(<em>Appears on:</em>
<a href="#sparkoperator.k8s.io/v1beta2.SparkPodSpec">SparkPodSpec</a>)
</p>
<p>
<p>NamePath is a pair of a name and a path to which the named objects should be mounted to.</p>
</p>
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>name</code></br>
<em>
string
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>path</code></br>
<em>
string
</em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="sparkoperator.k8s.io/v1beta2.PrometheusSpec">PrometheusSpec
</h3>
<p>
(<em>Appears on:</em>
<a href="#sparkoperator.k8s.io/v1beta2.MonitoringSpec">MonitoringSpec</a>)
</p>
<p>
<p>PrometheusSpec defines the Prometheus specification when Prometheus is to be used for
collecting and exposing metrics.</p>
</p>
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>jmxExporterJar</code></br>
<em>
string
</em>
</td>
<td>
<p>JmxExporterJar is the path to the Prometheus JMX exporter jar in the container.</p>
</td>
</tr>
<tr>
<td>
<code>port</code></br>
<em>
int32
</em>
</td>
<td>
<em>(Optional)</em>
<p>Port is the port of the HTTP server run by the Prometheus JMX exporter.
If not specified, 8090 will be used as the default.</p>
</td>
</tr>
<tr>
<td>
<code>configFile</code></br>
<em>
string
</em>
</td>
<td>
<p>ConfigFile is the path to the custom Prometheus configuration file provided in the Spark image.
ConfigFile takes precedence over Configuration, which is shown below.</p>
</td>
</tr>
<tr>
<td>
<code>configuration</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>Configuration is the content of the Prometheus configuration needed by the Prometheus JMX exporter.
If not specified, the content in spark-docker/conf/prometheus.yaml will be used.
Configuration has no effect if ConfigFile is set.</p>
</td>
</tr>
</tbody>
</table>
<h3 id="sparkoperator.k8s.io/v1beta2.RestartPolicy">RestartPolicy
</h3>
<p>
(<em>Appears on:</em>
<a href="#sparkoperator.k8s.io/v1beta2.SparkApplicationSpec">SparkApplicationSpec</a>)
</p>
<p>
<p>RestartPolicy is the policy of if and in which conditions the controller should restart a terminated application.
This completely defines actions to be taken on any kind of Failures during an application run.</p>
</p>
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>type</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.RestartPolicyType">
RestartPolicyType
</a>
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>onSubmissionFailureRetries</code></br>
<em>
int32
</em>
</td>
<td>
<p>FailureRetries are the number of times to retry a failed application before giving up in a particular case.
This is best effort and actual retry attempts can be &gt;= the value specified due to caching.
These are required if RestartPolicy is OnFailure.</p>
</td>
</tr>
<tr>
<td>
<code>onFailureRetries</code></br>
<em>
int32
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>onSubmissionFailureRetryInterval</code></br>
<em>
int64
</em>
</td>
<td>
<p>Interval to wait between successive retries of a failed application.</p>
</td>
</tr>
<tr>
<td>
<code>onFailureRetryInterval</code></br>
<em>
int64
</em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="sparkoperator.k8s.io/v1beta2.RestartPolicyType">RestartPolicyType
(<code>string</code> alias)</p></h3>
<p>
(<em>Appears on:</em>
<a href="#sparkoperator.k8s.io/v1beta2.RestartPolicy">RestartPolicy</a>)
</p>
<p>
</p>
<h3 id="sparkoperator.k8s.io/v1beta2.ScheduleState">ScheduleState
(<code>string</code> alias)</p></h3>
<p>
(<em>Appears on:</em>
<a href="#sparkoperator.k8s.io/v1beta2.ScheduledSparkApplicationStatus">ScheduledSparkApplicationStatus</a>)
</p>
<p>
</p>
<h3 id="sparkoperator.k8s.io/v1beta2.ScheduledSparkApplicationSpec">ScheduledSparkApplicationSpec
</h3>
<p>
(<em>Appears on:</em>
<a href="#sparkoperator.k8s.io/v1beta2.ScheduledSparkApplication">ScheduledSparkApplication</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>schedule</code></br>
<em>
string
</em>
</td>
<td>
<p>Schedule is a cron schedule on which the application should run.</p>
</td>
</tr>
<tr>
<td>
<code>template</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.SparkApplicationSpec">
SparkApplicationSpec
</a>
</em>
</td>
<td>
<p>Template is a template from which SparkApplication instances can be created.</p>
</td>
</tr>
<tr>
<td>
<code>suspend</code></br>
<em>
bool
</em>
</td>
<td>
<em>(Optional)</em>
<p>Suspend is a flag telling the controller to suspend subsequent runs of the application if set to true.
Defaults to false.</p>
</td>
</tr>
<tr>
<td>
<code>concurrencyPolicy</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.ConcurrencyPolicy">
ConcurrencyPolicy
</a>
</em>
</td>
<td>
<p>ConcurrencyPolicy is the policy governing concurrent SparkApplication runs.</p>
</td>
</tr>
<tr>
<td>
<code>successfulRunHistoryLimit</code></br>
<em>
int32
</em>
</td>
<td>
<em>(Optional)</em>
<p>SuccessfulRunHistoryLimit is the number of past successful runs of the application to keep.
Defaults to 1.</p>
</td>
</tr>
<tr>
<td>
<code>failedRunHistoryLimit</code></br>
<em>
int32
</em>
</td>
<td>
<em>(Optional)</em>
<p>FailedRunHistoryLimit is the number of past failed runs of the application to keep.
Defaults to 1.</p>
</td>
</tr>
</tbody>
</table>
<h3 id="sparkoperator.k8s.io/v1beta2.ScheduledSparkApplicationStatus">ScheduledSparkApplicationStatus
</h3>
<p>
(<em>Appears on:</em>
<a href="#sparkoperator.k8s.io/v1beta2.ScheduledSparkApplication">ScheduledSparkApplication</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>lastRun</code></br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#time-v1-meta">
Kubernetes meta/v1.Time
</a>
</em>
</td>
<td>
<p>LastRun is the time when the last run of the application started.</p>
</td>
</tr>
<tr>
<td>
<code>nextRun</code></br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#time-v1-meta">
Kubernetes meta/v1.Time
</a>
</em>
</td>
<td>
<p>NextRun is the time when the next run of the application will start.</p>
</td>
</tr>
<tr>
<td>
<code>lastRunName</code></br>
<em>
string
</em>
</td>
<td>
<p>LastRunName is the name of the SparkApplication for the most recent run of the application.</p>
</td>
</tr>
<tr>
<td>
<code>pastSuccessfulRunNames</code></br>
<em>
[]string
</em>
</td>
<td>
<p>PastSuccessfulRunNames keeps the names of SparkApplications for past successful runs.</p>
</td>
</tr>
<tr>
<td>
<code>pastFailedRunNames</code></br>
<em>
[]string
</em>
</td>
<td>
<p>PastFailedRunNames keeps the names of SparkApplications for past failed runs.</p>
</td>
</tr>
<tr>
<td>
<code>scheduleState</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.ScheduleState">
ScheduleState
</a>
</em>
</td>
<td>
<p>ScheduleState is the current scheduling state of the application.</p>
</td>
</tr>
<tr>
<td>
<code>reason</code></br>
<em>
string
</em>
</td>
<td>
<p>Reason tells why the ScheduledSparkApplication is in the particular ScheduleState.</p>
</td>
</tr>
</tbody>
</table>
<h3 id="sparkoperator.k8s.io/v1beta2.SecretInfo">SecretInfo
</h3>
<p>
(<em>Appears on:</em>
<a href="#sparkoperator.k8s.io/v1beta2.SparkPodSpec">SparkPodSpec</a>)
</p>
<p>
<p>SecretInfo captures information of a secret.</p>
</p>
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>name</code></br>
<em>
string
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>path</code></br>
<em>
string
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>secretType</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.SecretType">
SecretType
</a>
</em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="sparkoperator.k8s.io/v1beta2.SecretType">SecretType
(<code>string</code> alias)</p></h3>
<p>
(<em>Appears on:</em>
<a href="#sparkoperator.k8s.io/v1beta2.SecretInfo">SecretInfo</a>)
</p>
<p>
<p>SecretType tells the type of a secret.</p>
</p>
<h3 id="sparkoperator.k8s.io/v1beta2.SparkApplicationSpec">SparkApplicationSpec
</h3>
<p>
(<em>Appears on:</em>
<a href="#sparkoperator.k8s.io/v1beta2.SparkApplication">SparkApplication</a>, 
<a href="#sparkoperator.k8s.io/v1beta2.ScheduledSparkApplicationSpec">ScheduledSparkApplicationSpec</a>)
</p>
<p>
<p>SparkApplicationSpec describes the specification of a Spark application using Kubernetes as a cluster manager.
It carries every pieces of information a spark-submit command takes and recognizes.</p>
</p>
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>type</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.SparkApplicationType">
SparkApplicationType
</a>
</em>
</td>
<td>
<p>Type tells the type of the Spark application.</p>
</td>
</tr>
<tr>
<td>
<code>sparkVersion</code></br>
<em>
string
</em>
</td>
<td>
<p>SparkVersion is the version of Spark the application uses.</p>
</td>
</tr>
<tr>
<td>
<code>mode</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.DeployMode">
DeployMode
</a>
</em>
</td>
<td>
<p>Mode is the deployment mode of the Spark application.</p>
</td>
</tr>
<tr>
<td>
<code>image</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>Image is the container image for the driver, executor, and init-container. Any custom container images for the
driver, executor, or init-container takes precedence over this.</p>
</td>
</tr>
<tr>
<td>
<code>initContainerImage</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>InitContainerImage is the image of the init-container to use. Overrides Spec.Image if set.</p>
</td>
</tr>
<tr>
<td>
<code>imagePullPolicy</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>ImagePullPolicy is the image pull policy for the driver, executor, and init-container.</p>
</td>
</tr>
<tr>
<td>
<code>imagePullSecrets</code></br>
<em>
[]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>ImagePullSecrets is the list of image-pull secrets.</p>
</td>
</tr>
<tr>
<td>
<code>mainClass</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>MainClass is the fully-qualified main class of the Spark application.
This only applies to Java/Scala Spark applications.</p>
</td>
</tr>
<tr>
<td>
<code>mainApplicationFile</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>MainFile is the path to a bundled JAR, Python, or R file of the application.</p>
</td>
</tr>
<tr>
<td>
<code>arguments</code></br>
<em>
[]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>Arguments is a list of arguments to be passed to the application.</p>
</td>
</tr>
<tr>
<td>
<code>sparkConf</code></br>
<em>
map[string]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>SparkConf carries user-specified Spark configuration properties as they would use the  &ldquo;&ndash;conf&rdquo; option in
spark-submit.</p>
</td>
</tr>
<tr>
<td>
<code>hadoopConf</code></br>
<em>
map[string]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>HadoopConf carries user-specified Hadoop configuration properties as they would use the  the &ldquo;&ndash;conf&rdquo; option
in spark-submit.  The SparkApplication controller automatically adds prefix &ldquo;spark.hadoop.&rdquo; to Hadoop
configuration properties.</p>
</td>
</tr>
<tr>
<td>
<code>sparkConfigMap</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>SparkConfigMap carries the name of the ConfigMap containing Spark configuration files such as log4j.properties.
The controller will add environment variable SPARK_CONF_DIR to the path where the ConfigMap is mounted to.</p>
</td>
</tr>
<tr>
<td>
<code>hadoopConfigMap</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>HadoopConfigMap carries the name of the ConfigMap containing Hadoop configuration files such as core-site.xml.
The controller will add environment variable HADOOP_CONF_DIR to the path where the ConfigMap is mounted to.</p>
</td>
</tr>
<tr>
<td>
<code>volumes</code></br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#volume-v1-core">
[]Kubernetes core/v1.Volume
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Volumes is the list of Kubernetes volumes that can be mounted by the driver and/or executors.</p>
</td>
</tr>
<tr>
<td>
<code>driver</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.DriverSpec">
DriverSpec
</a>
</em>
</td>
<td>
<p>Driver is the driver specification.</p>
</td>
</tr>
<tr>
<td>
<code>executor</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.ExecutorSpec">
ExecutorSpec
</a>
</em>
</td>
<td>
<p>Executor is the executor specification.</p>
</td>
</tr>
<tr>
<td>
<code>deps</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.Dependencies">
Dependencies
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Deps captures all possible types of dependencies of a Spark application.</p>
</td>
</tr>
<tr>
<td>
<code>restartPolicy</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.RestartPolicy">
RestartPolicy
</a>
</em>
</td>
<td>
<p>RestartPolicy defines the policy on if and in which conditions the controller should restart an application.</p>
</td>
</tr>
<tr>
<td>
<code>nodeSelector</code></br>
<em>
map[string]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>NodeSelector is the Kubernetes node selector to be added to the driver and executor pods.
This field is mutually exclusive with nodeSelector at podSpec level (driver or executor).
This field will be deprecated in future versions (at SparkApplicationSpec level).</p>
</td>
</tr>
<tr>
<td>
<code>failureRetries</code></br>
<em>
int32
</em>
</td>
<td>
<em>(Optional)</em>
<p>FailureRetries is the number of times to retry a failed application before giving up.
This is best effort and actual retry attempts can be &gt;= the value specified.</p>
</td>
</tr>
<tr>
<td>
<code>retryInterval</code></br>
<em>
int64
</em>
</td>
<td>
<em>(Optional)</em>
<p>RetryInterval is the unit of intervals in seconds between submission retries.</p>
</td>
</tr>
<tr>
<td>
<code>pythonVersion</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>This sets the major Python version of the docker
image used to run the driver and executor containers. Can either be 2 or 3, default 2.</p>
</td>
</tr>
<tr>
<td>
<code>memoryOverheadFactor</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>This sets the Memory Overhead Factor that will allocate memory to non-JVM memory.
For JVM-based jobs this value will default to 0.10, for non-JVM jobs 0.40. Value of this field will
be overridden by <code>Spec.Driver.MemoryOverhead</code> and <code>Spec.Executor.MemoryOverhead</code> if they are set.</p>
</td>
</tr>
<tr>
<td>
<code>monitoring</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.MonitoringSpec">
MonitoringSpec
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Monitoring configures how monitoring is handled.</p>
</td>
</tr>
<tr>
<td>
<code>batchScheduler</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>BatchScheduler configures which batch scheduler will be used for scheduling</p>
</td>
</tr>
<tr>
<td>
<code>timeToLiveSeconds</code></br>
<em>
int64
</em>
</td>
<td>
<em>(Optional)</em>
<p>TimeToLiveSeconds defines the Time-To-Live (TTL) duration in seconds for this SparkAplication
after its termination.
The SparkApplication object will be garbage collected if the current time is more than the
TimeToLiveSeconds since its termination.</p>
</td>
</tr>
<tr>
<td>
<code>batchSchedulerOptions</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.BatchSchedulerConfiguration">
BatchSchedulerConfiguration
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>BatchSchedulerOptions provides fine-grained control on how to batch scheduling.</p>
</td>
</tr>
</tbody>
</table>
<h3 id="sparkoperator.k8s.io/v1beta2.SparkApplicationStatus">SparkApplicationStatus
</h3>
<p>
(<em>Appears on:</em>
<a href="#sparkoperator.k8s.io/v1beta2.SparkApplication">SparkApplication</a>)
</p>
<p>
<p>SparkApplicationStatus describes the current status of a Spark application.</p>
</p>
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>sparkApplicationId</code></br>
<em>
string
</em>
</td>
<td>
<p>SparkApplicationID is set by the spark-distribution(via spark.app.id config) on the driver and executor pods</p>
</td>
</tr>
<tr>
<td>
<code>submissionID</code></br>
<em>
string
</em>
</td>
<td>
<p>SubmissionID is a unique ID of the current submission of the application.</p>
</td>
</tr>
<tr>
<td>
<code>lastSubmissionAttemptTime</code></br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#time-v1-meta">
Kubernetes meta/v1.Time
</a>
</em>
</td>
<td>
<p>LastSubmissionAttemptTime is the time for the last application submission attempt.</p>
</td>
</tr>
<tr>
<td>
<code>terminationTime</code></br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#time-v1-meta">
Kubernetes meta/v1.Time
</a>
</em>
</td>
<td>
<p>CompletionTime is the time when the application runs to completion if it does.</p>
</td>
</tr>
<tr>
<td>
<code>driverInfo</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.DriverInfo">
DriverInfo
</a>
</em>
</td>
<td>
<p>DriverInfo has information about the driver.</p>
</td>
</tr>
<tr>
<td>
<code>applicationState</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.ApplicationState">
ApplicationState
</a>
</em>
</td>
<td>
<p>AppState tells the overall application state.</p>
</td>
</tr>
<tr>
<td>
<code>executorState</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.ExecutorState">
map[string]github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2.ExecutorState
</a>
</em>
</td>
<td>
<p>ExecutorState records the state of executors by executor Pod names.</p>
</td>
</tr>
<tr>
<td>
<code>executionAttempts</code></br>
<em>
int32
</em>
</td>
<td>
<p>ExecutionAttempts is the total number of attempts to run a submitted application to completion.
Incremented upon each attempted run of the application and reset upon invalidation.</p>
</td>
</tr>
<tr>
<td>
<code>submissionAttempts</code></br>
<em>
int32
</em>
</td>
<td>
<p>SubmissionAttempts is the total number of attempts to submit an application to run.
Incremented upon each attempted submission of the application and reset upon invalidation and rerun.</p>
</td>
</tr>
</tbody>
</table>
<h3 id="sparkoperator.k8s.io/v1beta2.SparkApplicationType">SparkApplicationType
(<code>string</code> alias)</p></h3>
<p>
(<em>Appears on:</em>
<a href="#sparkoperator.k8s.io/v1beta2.SparkApplicationSpec">SparkApplicationSpec</a>)
</p>
<p>
<p>SparkApplicationType describes the type of a Spark application.</p>
</p>
<h3 id="sparkoperator.k8s.io/v1beta2.SparkPodSpec">SparkPodSpec
</h3>
<p>
(<em>Appears on:</em>
<a href="#sparkoperator.k8s.io/v1beta2.DriverSpec">DriverSpec</a>, 
<a href="#sparkoperator.k8s.io/v1beta2.ExecutorSpec">ExecutorSpec</a>)
</p>
<p>
<p>SparkPodSpec defines common things that can be customized for a Spark driver or executor pod.
TODO: investigate if we should use v1.PodSpec and limit what can be set instead.</p>
</p>
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>cores</code></br>
<em>
int32
</em>
</td>
<td>
<em>(Optional)</em>
<p>Cores is the number of CPU cores to request for the pod.</p>
</td>
</tr>
<tr>
<td>
<code>coreLimit</code></br>
<em>
string
</em>
</td>
<td>
<p>CoreLimit specifies a hard limit on CPU cores for the pod.
Optional</p>
</td>
</tr>
<tr>
<td>
<code>memory</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>Memory is the amount of memory to request for the pod.</p>
</td>
</tr>
<tr>
<td>
<code>memoryOverhead</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>MemoryOverhead is the amount of off-heap memory to allocate in cluster mode, in MiB unless otherwise specified.</p>
</td>
</tr>
<tr>
<td>
<code>gpu</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.GPUSpec">
GPUSpec
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>GPU specifies GPU requirement for the pod.</p>
</td>
</tr>
<tr>
<td>
<code>image</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>Image is the container image to use. Overrides Spec.Image if set.</p>
</td>
</tr>
<tr>
<td>
<code>configMaps</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.NamePath">
[]NamePath
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>ConfigMaps carries information of other ConfigMaps to add to the pod.</p>
</td>
</tr>
<tr>
<td>
<code>secrets</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.SecretInfo">
[]SecretInfo
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Secrets carries information of secrets to add to the pod.</p>
</td>
</tr>
<tr>
<td>
<code>env</code></br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#envvar-v1-core">
[]Kubernetes core/v1.EnvVar
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Env carries the environment variables to add to the pod.</p>
</td>
</tr>
<tr>
<td>
<code>envVars</code></br>
<em>
map[string]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>EnvVars carries the environment variables to add to the pod.
DEPRECATED.</p>
</td>
</tr>
<tr>
<td>
<code>envSecretKeyRefs</code></br>
<em>
<a href="#sparkoperator.k8s.io/v1beta2.NameKey">
map[string]github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2.NameKey
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>EnvSecretKeyRefs holds a mapping from environment variable names to SecretKeyRefs.
DEPRECATED.</p>
</td>
</tr>
<tr>
<td>
<code>labels</code></br>
<em>
map[string]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>Labels are the Kubernetes labels to be added to the pod.</p>
</td>
</tr>
<tr>
<td>
<code>annotations</code></br>
<em>
map[string]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>Annotations are the Kubernetes annotations to be added to the pod.</p>
</td>
</tr>
<tr>
<td>
<code>volumeMounts</code></br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#volumemount-v1-core">
[]Kubernetes core/v1.VolumeMount
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>VolumeMounts specifies the volumes listed in &ldquo;.spec.volumes&rdquo; to mount into the main container&rsquo;s filesystem.</p>
</td>
</tr>
<tr>
<td>
<code>affinity</code></br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#affinity-v1-core">
Kubernetes core/v1.Affinity
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Affinity specifies the affinity/anti-affinity settings for the pod.</p>
</td>
</tr>
<tr>
<td>
<code>tolerations</code></br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#toleration-v1-core">
[]Kubernetes core/v1.Toleration
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Tolerations specifies the tolerations listed in &ldquo;.spec.tolerations&rdquo; to be applied to the pod.</p>
</td>
</tr>
<tr>
<td>
<code>securityContext</code></br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#podsecuritycontext-v1-core">
Kubernetes core/v1.PodSecurityContext
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>SecurityContenxt specifies the PodSecurityContext to apply.</p>
</td>
</tr>
<tr>
<td>
<code>schedulerName</code></br>
<em>
string
</em>
</td>
<td>
<em>(Optional)</em>
<p>SchedulerName specifies the scheduler that will be used for scheduling</p>
</td>
</tr>
<tr>
<td>
<code>sidecars</code></br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#container-v1-core">
[]Kubernetes core/v1.Container
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>Sidecars is a list of sidecar containers that run along side the main Spark container.</p>
</td>
</tr>
<tr>
<td>
<code>initContainers</code></br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#container-v1-core">
[]Kubernetes core/v1.Container
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>InitContainers is a list of init-containers that run to completion before the main Spark container.</p>
</td>
</tr>
<tr>
<td>
<code>hostNetwork</code></br>
<em>
bool
</em>
</td>
<td>
<em>(Optional)</em>
<p>HostNetwork indicates whether to request host networking for the pod or not.</p>
</td>
</tr>
<tr>
<td>
<code>nodeSelector</code></br>
<em>
map[string]string
</em>
</td>
<td>
<em>(Optional)</em>
<p>NodeSelector is the Kubernetes node selector to be added to the driver and executor pods.
This field is mutually exclusive with nodeSelector at SparkApplication level (which will be deprecated).</p>
</td>
</tr>
<tr>
<td>
<code>dnsConfig</code></br>
<em>
<a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#poddnsconfig-v1-core">
Kubernetes core/v1.PodDNSConfig
</a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>DnsConfig dns settings for the pod, following the Kubernetes specifications.</p>
</td>
</tr>
</tbody>
</table>
<hr/>
<p><em>
Generated with <code>gen-crd-api-reference-docs</code>
on git commit <code>988409b</code>.
</em></p>
