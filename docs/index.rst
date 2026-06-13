.. meta::
   :description: Kubeflow Spark Operator — run Apache Spark applications on Kubernetes the idiomatic way
   :keywords: kubeflow, spark, spark operator, apache spark, kubernetes, big data, data engineering

.. raw:: html

   <div class="landing-page">

   <section class="hero">
   <div class="hero-bg-pattern"></div>
   <div class="hero-content">
   <div class="hero-badge">Open Source &middot; CNCF &middot; Kubeflow</div>
   <h1 class="hero-title">Kubeflow<br><span class="hero-title-line2">Spark Operator</span></h1>
   <p class="hero-tagline">Run Apache Spark applications on Kubernetes as easily and idiomatically as any other workload &mdash; declaratively, with a single custom resource.</p>
   <div class="hero-actions">
   <a href="getting-started/index.html" class="btn btn-primary">Get Started <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round"><path d="M5 12h14M12 5l7 7-7 7"/></svg></a>
   <a href="https://github.com/kubeflow/spark-operator" class="btn btn-secondary">GitHub</a>
   </div>
   <div class="hero-sub">Submit Spark jobs with <code style="color:rgba(255,255,255,0.75);background:rgba(255,255,255,0.08)">kubectl apply</code> &mdash; the operator handles <code style="color:rgba(255,255,255,0.75);background:rgba(255,255,255,0.08)">spark-submit</code> for you.</div>
   </div>
   </section>

   <section class="what-is">
   <h2 class="section-title">What is Kubeflow Spark Operator?</h2>
   <p class="section-desc">The Kubernetes Operator for Apache Spark makes specifying and running Spark applications as easy and idiomatic as running other workloads on Kubernetes. It uses Kubernetes custom resources to specify, run, and surface the status of Spark applications. Define your job in a YAML <code>SparkApplication</code>, apply it to your cluster, and the operator automatically runs <code>spark-submit</code>, tracks driver and executor pods, restarts on failure, schedules cron jobs, and exports metrics &mdash; all without dealing with the Spark submission process yourself.</p>
   <p class="section-desc">Kubeflow Spark Operator is a core component of <a href="https://www.kubeflow.org/">Kubeflow</a> &mdash; the open-source foundation of tools for AI platforms on Kubernetes.</p>
   </section>

   <section class="features">
   <h2 class="section-title">Why Spark Operator?</h2>
   <div class="features-grid">

   <div class="feature-card">
   <div class="feature-icon"><svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="var(--lp-blue)" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="8" y1="13" x2="16" y2="13"/><line x1="8" y1="17" x2="16" y2="17"/></svg></div>
   <h3>Declarative Applications</h3>
   <p>Specify Spark applications declaratively with the <code>SparkApplication</code> custom resource and manage them through the Kubernetes API &mdash; no manual <code>spark-submit</code> required.</p>
   </div>

   <div class="feature-card">
   <div class="feature-icon"><svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="var(--lp-blue)" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><polyline points="12 7 12 12 15 14"/></svg></div>
   <h3>Native Cron Scheduling</h3>
   <p>Run Spark jobs on a schedule with <code>ScheduledSparkApplication</code>, including configurable concurrency policies and history limits.</p>
   </div>

   <div class="feature-card">
   <div class="feature-icon"><svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="var(--lp-blue)" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><path d="M9 12l2 2 4-4"/></svg></div>
   <h3>Pod Customization</h3>
   <p>Customize driver and executor pods beyond native Spark via the mutating admission webhook &mdash; mount ConfigMaps and volumes, set affinity, tolerations, and more.</p>
   </div>

   <div class="feature-card">
   <div class="feature-icon"><svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="var(--lp-blue)" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M3 3v18h18"/><polyline points="7 14 11 10 14 13 20 7"/></svg></div>
   <h3>Metrics &amp; Monitoring</h3>
   <p>Collect and export application-level and driver/executor metrics to Prometheus, with optional JMX exporter integration for deep observability.</p>
   </div>

   <div class="feature-card">
   <div class="feature-icon"><svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="var(--lp-blue)" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/></svg></div>
   <h3>Batch Scheduling</h3>
   <p>Integrate with Volcano, Apache YuniKorn, and the Kubernetes scheduler plugins for gang scheduling and resource-aware placement of Spark workloads.</p>
   </div>

   <div class="feature-card">
   <div class="feature-icon"><svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="var(--lp-blue)" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/><polyline points="3.27 6.96 12 12.01 20.73 6.96"/><line x1="12" y1="22.08" x2="12" y2="12"/></svg></div>
   <h3>Production Ready</h3>
   <p>A graduated part of the Kubeflow ecosystem, backed by the CNCF community. Automatic restarts, retries with back-off, and re-submission for updated specs.</p>
   </div>

   </div>
   </section>

   <section class="frameworks">
   <h2 class="section-title">Core Capabilities</h2>
   <div class="framework-grid">
   <div class="framework-chip">SparkApplication</div>
   <div class="framework-chip">ScheduledSparkApplication</div>
   <div class="framework-chip">Spark Connect</div>
   <div class="framework-chip">Dynamic Allocation</div>
   <div class="framework-chip">Mutating Webhook</div>
   <div class="framework-chip">Volcano</div>
   <div class="framework-chip">YuniKorn</div>
   <div class="framework-chip">Prometheus</div>
   </div>
   </section>

   <section class="architecture">
   <h2 class="section-title">How It Works</h2>
   <p class="section-desc">You submit a declarative <code>SparkApplication</code> with <code>kubectl</code>; the operator reconciles it &mdash; building the submission, running <code>spark-submit</code>, and monitoring the driver and executor pods until the job completes.</p>
   <figure class="arch-figure">
   <img src="_static/img/sparkoperator-arch.jpg" alt="Kubeflow Spark Operator architecture: a user submits a SparkApplication, the operator's controller, submission runner, pod monitor, and mutating admission webhook reconcile it against the Kubernetes API, and the driver pod launches executor pods to run the job.">
   </figure>
   </section>

   <section class="video-section">
   <h2 class="section-title">See It in Action</h2>
   <p class="section-desc">Watch an introduction to running Apache Spark on Kubernetes with the Kubeflow Spark Operator.</p>
   <div class="video-wrap">
   <iframe src="https://www.youtube-nocookie.com/embed/5m05fOVajto" title="Kubeflow Spark Operator" loading="lazy" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>
   </div>
   </section>

   <section class="doc-nav">
   <h2 class="section-title">Documentation</h2>
   <p class="section-desc">Everything you need &mdash; from your first Spark job to production operation and contributing.</p>
   <div class="doc-cards">
   <a class="doc-card" href="overview/index.html">
   <strong>Overview</strong>
   <p>Learn what the Spark Operator is, its architecture, and how it manages the lifecycle of Spark applications</p>
   </a>
   <a class="doc-card" href="getting-started/index.html">
   <strong>Getting Started</strong>
   <p>Install the operator with Helm and run your first SparkApplication in minutes</p>
   </a>
   <a class="doc-card" href="user-guide/index.html">
   <strong>User Guide</strong>
   <p>Write, configure, schedule, monitor, and operate SparkApplications in depth</p>
   </a>
   <a class="doc-card" href="performance/index.html">
   <strong>Performance</strong>
   <p>Benchmarking results and tuning guidance for high-throughput Spark workloads</p>
   </a>
   <a class="doc-card" href="reference/index.html">
   <strong>Reference</strong>
   <p>The <code>v1beta2</code> API definition for SparkApplication and ScheduledSparkApplication</p>
   </a>
   <a class="doc-card" href="contributor-guide/index.html">
   <strong>Contributor Guide</strong>
   <p>Set up your development environment and contribute to the Spark Operator project</p>
   </a>
   </div>
   </section>

   <section class="quickstart">
   <h2 class="section-title">Submit a Spark Job in One File</h2>
   <div class="code-block-wrapper">
   <div class="code-lang">yaml</div>
   <pre class="landing-code"><code><span class="hl-cm"># spark-pi.yaml</span>&#10;<span class="hl-fn">apiVersion</span>: sparkoperator.k8s.io/v1beta2&#10;<span class="hl-fn">kind</span>: <span class="hl-kw">SparkApplication</span>&#10;<span class="hl-fn">metadata</span>:&#10;  <span class="hl-fn">name</span>: spark-pi&#10;  <span class="hl-fn">namespace</span>: default&#10;<span class="hl-fn">spec</span>:&#10;  <span class="hl-fn">type</span>: Scala&#10;  <span class="hl-fn">mode</span>: cluster&#10;  <span class="hl-fn">image</span>: spark:<span class="hl-num">4.0.1</span>&#10;  <span class="hl-fn">mainClass</span>: org.apache.spark.examples.SparkPi&#10;  <span class="hl-fn">mainApplicationFile</span>: local:///opt/spark/examples/jars/spark-examples.jar&#10;  <span class="hl-fn">sparkVersion</span>: <span class="hl-str">"4.0.1"</span>&#10;  <span class="hl-fn">driver</span>:&#10;    <span class="hl-fn">cores</span>: <span class="hl-num">1</span>&#10;    <span class="hl-fn">memory</span>: <span class="hl-str">"512m"</span>&#10;  <span class="hl-fn">executor</span>:&#10;    <span class="hl-fn">instances</span>: <span class="hl-num">2</span>&#10;    <span class="hl-fn">cores</span>: <span class="hl-num">1</span>&#10;    <span class="hl-fn">memory</span>: <span class="hl-str">"512m"</span></code></pre>
   </div>
   <p class="quickstart-note"><code>kubectl apply -f spark-pi.yaml</code> &mdash; the operator runs spark-submit and tracks the job for you. <a href="getting-started/index.html">See the full quickstart &rarr;</a></p>
   </section>

   <section class="community">
   <h2 class="section-title">Join the Community</h2>
   <p class="section-desc">Kubeflow Spark Operator is an open and welcoming community of developers, data engineers, and organizations &mdash; part of the Cloud Native Computing Foundation.</p>
   <div class="community-links">
   <a href="https://github.com/kubeflow/spark-operator" class="community-card"><span class="comm-icon"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M12 .5C5.37.5 0 5.78 0 12.29c0 5.2 3.44 9.6 8.21 11.16.6.11.82-.25.82-.56v-2.1c-3.34.71-4.04-1.4-4.04-1.4-.55-1.36-1.34-1.72-1.34-1.72-1.09-.73.08-.72.08-.72 1.2.08 1.84 1.22 1.84 1.22 1.07 1.79 2.81 1.27 3.5.97.11-.76.42-1.27.76-1.56-2.67-.3-5.47-1.31-5.47-5.83 0-1.29.47-2.34 1.23-3.17-.12-.3-.53-1.52.12-3.16 0 0 1-.32 3.3 1.21a11.5 11.5 0 0 1 6 0c2.28-1.53 3.29-1.21 3.29-1.21.65 1.64.24 2.86.12 3.16.77.83 1.23 1.88 1.23 3.17 0 4.53-2.81 5.52-5.49 5.82.43.37.81 1.1.81 2.22v3.29c0 .31.22.68.83.56C20.57 21.88 24 17.49 24 12.29 24 5.78 18.63.5 12 .5z"/></svg></span><strong>GitHub</strong><span>Star, fork, and contribute</span></a>
   <a href="https://www.kubeflow.org/docs/about/community/" class="community-card"><span class="comm-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg></span><strong>Slack</strong><span>#kubeflow-spark-operator</span></a>
   <a href="https://slack.cncf.io/" class="community-card"><span class="comm-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/></svg></span><strong>CNCF Slack</strong><span>Join the CNCF Slack workspace</span></a>
   <a href="https://groups.google.com/g/kubeflow-discuss" class="community-card"><span class="comm-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 4h16a2 2 0 0 1 2 2v12a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2z"/><polyline points="22,6 12,13 2,6"/></svg></span><strong>Mailing List</strong><span>kubeflow-discuss</span></a>
   <a href="https://github.com/kubeflow/spark-operator/issues" class="community-card"><span class="comm-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg></span><strong>Issues</strong><span>Report bugs and request features</span></a>
   <a href="https://www.kubeflow.org/docs/components/spark-operator/" class="community-card"><span class="comm-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M2 3h6a4 4 0 0 1 4 4v14a3 3 0 0 0-3-3H2z"/><path d="M22 3h-6a4 4 0 0 0-4 4v14a3 3 0 0 1 3-3h7z"/></svg></span><strong>Kubeflow.org</strong><span>Official Kubeflow documentation</span></a>
   <a href="https://github.com/kubeflow/spark-operator/blob/master/CONTRIBUTING.md" class="community-card"><span class="comm-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg></span><strong>Contributing</strong><span>How to get involved</span></a>
   <a href="https://github.com/kubeflow/spark-operator/releases" class="community-card"><span class="comm-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20.59 13.41l-7.17 7.17a2 2 0 0 1-2.83 0L2 12V2h10l8.59 8.59a2 2 0 0 1 0 2.82z"/><line x1="7" y1="7" x2="7.01" y2="7"/></svg></span><strong>Releases</strong><span>Changelog and downloads</span></a>
   </div>
   </section>

   <footer class="landing-footer">
   <div class="footer-inner">
   <p class="footer-kubeflow">Kubeflow Spark Operator is part of <a href="https://www.kubeflow.org/">Kubeflow</a> &mdash; the foundation of tools for AI platforms on Kubernetes.</p>
   <a class="footer-cncf-link" href="https://www.cncf.io/"><img class="footer-cncf" src="_static/img/cncf-white.svg" alt="Cloud Native Computing Foundation"></a>
   <p>We are a <a href="https://www.cncf.io/">Cloud Native Computing Foundation</a> project.</p>
   <p class="footer-copy">&copy; 2026 The Kubeflow Authors &middot; Documentation distributed under CC BY 4.0</p>
   </div>
   </footer>

   </div>

.. toctree::
   :hidden:
   :maxdepth: 3

   overview/index
   getting-started/index
   user-guide/index
   performance/index
   reference/index
   contributor-guide/index
