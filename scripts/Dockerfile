FROM ubuntu:latest

COPY xline /usr/local/bin
COPY benchmark /usr/local/bin
COPY start_xline.sh /usr/local/bin

RUN apt-get update && apt-get install -y iproute2 iputils-ping


CMD ["/bin/bash", "/usr/local/bin/start_xline.sh"]
