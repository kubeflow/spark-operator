FROM scratch
ADD spark-operator /usr/bin
ENTRYPOINT ["/user/bin/spark-operator"]
