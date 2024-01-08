#!/usr/bin/bash -x
DIR=$(cd $(dirname $0); pwd)

CA_KEY=${DIR}/certs/ca.key
CA_CRT=${DIR}/certs/ca.crt

SERVER_KEY=${DIR}/certs/server.key
SERVER_CSR=${DIR}/certs/server.csr
SERVER_CRT=${DIR}/certs/server.crt

CLIENT_KEY=${DIR}/certs/client.key
CLIENT_CSR=${DIR}/certs/client.csr
CLIENT_CRT=${DIR}/certs/client.crt

OPENSSL_CONF=${DIR}/certs/openssl.conf

DAYS=3650

[ -d ${DIR}/certs ] || mkdir -p ${DIR}/certs

cat > ${OPENSSL_CONF} << EOF
[req]
req_extensions = v3_req
distinguished_name = req_distinguished_name
[req_distinguished_name]
[v3_req]
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names
[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
IP.2 = 0.0.0.0
EOF

[ -f ${CA_KEY} ] || openssl genrsa -out ${CA_KEY} 2048 || exit 1
[ -f ${CA_CRT} ] || openssl req -x509 -new -nodes -key ${CA_KEY} -subj "/CN=ca" -days ${DAYS} -out ${CA_CRT} || exit 1


[ -f ${SERVER_KEY} ] || openssl genrsa -out ${SERVER_KEY} 2048 || exit 1
[ -f ${SERVER_CSR} ] || openssl req -new -key ${SERVER_KEY} -subj "/CN=server" -out ${SERVER_CSR} -config ${OPENSSL_CONF} || exit 1
[ -f ${SERVER_CRT} ] || openssl x509 -req -in ${SERVER_CSR} -CA ${CA_CRT} -CAkey ${CA_KEY} -CAcreateserial -out ${SERVER_CRT} -days ${DAYS} -extensions v3_req  -extfile ${OPENSSL_CONF} || exit 1

[ -f ${CLIENT_KEY} ] || openssl genrsa -out ${CLIENT_KEY} 2048 || exit 1
[ -f ${CLIENT_CSR} ] || openssl req -new -key ${CLIENT_KEY} -subj "/CN=client" -out ${CLIENT_CSR} -config ${OPENSSL_CONF} || exit 1
[ -f ${CLIENT_CRT} ] || openssl x509 -req -in ${CLIENT_CSR} -CA ${CA_CRT} -CAkey ${CA_KEY} -CAcreateserial -out ${CLIENT_CRT} -days ${DAYS} -extensions v3_req  -extfile ${OPENSSL_CONF} || exit 1
