#/bin/bash
apt-get install -y cmake g++ expect

# install protoc
wget https://github.com/protocolbuffers/protobuf/releases/download/v21.10/protoc-21.10-linux-x86_64.zip
unzip protoc-21.10-linux-x86_64.zip -d .local
mv "$(pwd)/.local/bin/protoc" /bin/
