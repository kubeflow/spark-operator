package webhook

// Package webhook implements a mutating admission webhook for patching Spark driver and executor pods.
// The webhook supports mutations that are not supported by Spark on Kubernetes itself, e.g., adding an
// OwnerReference to the driver pod for the owning SparkApplications or setting the SecurityContext.
