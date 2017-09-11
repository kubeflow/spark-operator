FROM scratch
COPY spark-operator /usr/bin/
ENTRYPOINT ["/user/bin/spark-operator"]
