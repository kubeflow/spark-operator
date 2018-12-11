#!/bin/bash
arg1=$(head -2 /opt/spark/credentials | tail -1)
arg2=$(head -3 /opt/spark/credentials | tail -1)
arg3=$(head -1 /opt/spark/credentials | tail -1)

subscription-manager register --username=$arg1 --password=$arg2  --name=docker
subscription-manager attach --pool=$arg3 && \
yum install -y openssl
subscription-manager remove --al 
subscription-manager unregister
subscription-manager clean
