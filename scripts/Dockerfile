FROM ubuntu:latest

COPY xline /usr/local/bin
COPY benchmark /usr/local/bin

RUN apt-get update && apt-get install -y iproute2 iputils-ping

CMD ["/usr/local/bin/xline"]
