FROM ubuntu:latest

WORKDIR /xline

COPY xline .
COPY benchmark .
COPY etcdctl .
COPY etcd .

RUN apt-get update && apt-get install -y iproute2 iputils-ping dnsutils