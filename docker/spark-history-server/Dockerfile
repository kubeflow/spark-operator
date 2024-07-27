#
# Copyright 2024 The Kubeflow authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

ARG SPARK_IMAGE=spark:3.5.0

FROM ${SPARK_IMAGE}

# Add dependency for AWS S3 support
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar ${SPARK_HOME}/jars
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar ${SPARK_HOME}/jars

# Add dependency for Google Cloud Storage support
ADD https://repo1.maven.org/maven2/com/google/guava/guava/23.0/guava-23.0.jar ${SPARK_HOME}/jars
ADD https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar ${SPARK_HOME}/jars

# Add dependency for Azure Blob Storage support
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.4/hadoop-azure-3.3.4.jar ${SPARK_HOME}/jars
ADD https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/7.0.1/azure-storage-7.0.1.jar ${SPARK_HOME}/jars

# Add dependency for Alibaba Cloud OSS support
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aliyun/3.3.4/hadoop-aliyun-3.3.4.jar ${SPARK_HOME}/jars
ADD https://repo1.maven.org/maven2/com/aliyun/oss/aliyun-sdk-oss/3.18.0/aliyun-sdk-oss-3.18.0.jar ${SPARK_HOME}/jars

COPY entrypoint.sh /opt

ENTRYPOINT ["/opt/entrypoint.sh"]
