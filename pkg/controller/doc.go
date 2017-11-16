package controller

// Package controller implements the CustomResourceDefinition (CRD) controller for SparkApplications.
// The controller is responsible for watching SparkApplication objects and submitting Spark applications
// described by the specs in the objects on behalf of users. After an application is submitted, the
// controller monitors the application state and updates the status field of the SparkApplication object
// accordingly. The controller uses a SparkSubmitRunner to submit applications to run in the Kubernetes
// cluster where Spark Operator runs. The SparkSubmitRunner maintains a set of workers, each of which is
// a goroutine, for actually running the spark-submit commands. The controller also uses a SparkPodMonitor
// to watch Spark driver and executor pods. The SparkPodMonitor sends driver and executor state updates
// to the controller, which then updates status field of SparkApplication objects accordingly.
