package initializer

// Package initializer implements the initializer for Spark pods. It looks for certain annotations on
// the Spark driver and executor pods and modifies the pod spec based on the annotations. For example,
// it is able to inject user-specified secrets and ConfigMaps into the Spark pods. It can also set
// user-defined environment variables. It removes its name from the list of pending initializers from
// pods it has processed.
