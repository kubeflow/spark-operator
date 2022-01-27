# Onehouse-spark-operator

Forked On Aug-4-2021

# Changes
- Dockerfile is updated to extende from spark-hadoop image instead of just spark image

# Steps to create image

```
export AWS_PROFILE=prod_onehouse_debug

aws ecr get-login-password --region us-west-1 | docker login --username AWS --password-stdin 194159489498.dkr.ecr.us-west-1.amazonaws.com
docker build -t onehouse-spark-operator .
docker tag onehouse-spark-operator:latest 194159489498.dkr.ecr.us-west-1.amazonaws.com/onehouse-spark-operator:latest
docker push 194159489498.dkr.ecr.us-west-1.amazonaws.com/onehouse-spark-operator:latest
```