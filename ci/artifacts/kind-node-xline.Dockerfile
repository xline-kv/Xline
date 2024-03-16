ARG K8S_VERSION

FROM kindest/node:${K8S_VERSION}

RUN mkdir /tmp/kind
COPY xline.tar /tmp/kind/
RUN ( containerd -l warning & ) && ctr -n k8s.io images import --no-unpack /tmp/kind/*.tar
RUN rm /tmp/kind/*.tar
