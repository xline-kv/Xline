FROM ubuntu:latest

RUN apt-get update && apt-get install -y iproute2 iputils-ping

COPY xline /usr/local/bin
COPY benchmark /usr/local/bin

CMD ["/usr/local/bin/xline"]
