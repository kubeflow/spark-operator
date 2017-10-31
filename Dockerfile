FROM kubespark/spark-base:v2.2.0-kubernetes-0.5.0
COPY spark-operator /usr/bin/
ENTRYPOINT ["/user/bin/spark-operator"]
