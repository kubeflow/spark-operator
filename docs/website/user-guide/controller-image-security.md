# Controller Image Security

The Spark Operator controller image is built on a Spark base image. The default `SparkApplication` submission path also invokes `spark-submit` from that runtime.

As a result, vulnerability scans of the controller image can include operating-system packages and JVM dependencies inherited from the selected Spark distribution, in addition to components maintained directly by the Spark Operator project.

## Evaluating vulnerability findings

The appropriate remediation depends on where the affected component originates.

Findings in Spark Operator code or repository-managed dependencies are generally addressed in this project. Findings inherited from the selected Spark distribution may instead require an upstream update, a different compatible Spark distribution, or a custom controller image built from an appropriate Spark base.

Scanner-reported fixed versions should not be treated as instructions to replace individual JAR files without compatibility validation. Dependencies can be bundled or shaded inside other artifacts, and updating one component can require coordinated
dependency or build-tool changes.

## Replacing inherited dependencies

If a dependency replacement or rebuild is being considered, at minimum:

- Check whether the affected dependency is a standalone JAR or is bundled or shaded inside another artifact. Replacing a standalone JAR may not update embedded copies.
- Check related and transitive dependencies before applying a scanner-reported fixed version. Some updates may require coordinated dependency or build-tool changes.
- Rebuild and test the affected artifact with the target Spark distribution, then scan the resulting image again. A successful build alone does not establish runtime compatibility.

## Custom controller images

Organizations that require a different approved Spark base can build a custom controller image using the released Spark Operator binary.

See [Building Custom Operator Images](building-custom-images.md) for the supported custom-image workflow.
