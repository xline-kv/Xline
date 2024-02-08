#!/usr/bin/bash -x
DIR=$(cd $(dirname $0); pwd)

CA_KEY=${DIR}/certs/ca.key
CA_CRT=${DIR}/certs/ca.crt

SERVER_KEY=${DIR}/certs/server.key
SERVER_CSR=${DIR}/certs/server.csr
SERVER_CRT=${DIR}/certs/server.crt

ROOT_CLIENT_KEY=${DIR}/certs/root_client.key
ROOT_CLIENT_CSR=${DIR}/certs/root_client.csr
ROOT_CLIENT_CRT=${DIR}/certs/root_client.crt

U1_CLIENT_KEY=${DIR}/certs/u1_client.key
U1_CLIENT_CSR=${DIR}/certs/u1_client.csr
U1_CLIENT_CRT=${DIR}/certs/u1_client.crt

U2_CLIENT_KEY=${DIR}/certs/u2_client.key
U2_CLIENT_CSR=${DIR}/certs/u2_client.csr
U2_CLIENT_CRT=${DIR}/certs/u2_client.crt

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

[ -f ${ROOT_CLIENT_KEY} ] || openssl genrsa -out ${ROOT_CLIENT_KEY} 2048 || exit 1
[ -f ${ROOT_CLIENT_CSR} ] || openssl req -new -key ${ROOT_CLIENT_KEY} -subj "/CN=root" -out ${ROOT_CLIENT_CSR} -config ${OPENSSL_CONF} || exit 1
[ -f ${ROOT_CLIENT_CRT} ] || openssl x509 -req -in ${ROOT_CLIENT_CSR} -CA ${CA_CRT} -CAkey ${CA_KEY} -CAcreateserial -out ${ROOT_CLIENT_CRT} -days ${DAYS} -extensions v3_req  -extfile ${OPENSSL_CONF} || exit 1

[ -f ${U1_CLIENT_KEY} ] || openssl genrsa -out ${U1_CLIENT_KEY} 2048 || exit 1
[ -f ${U1_CLIENT_CSR} ] || openssl req -new -key ${U1_CLIENT_KEY} -subj "/CN=u1" -out ${U1_CLIENT_CSR} -config ${OPENSSL_CONF} || exit 1
[ -f ${U1_CLIENT_CRT} ] || openssl x509 -req -in ${U1_CLIENT_CSR} -CA ${CA_CRT} -CAkey ${CA_KEY} -CAcreateserial -out ${U1_CLIENT_CRT} -days ${DAYS} -extensions v3_req  -extfile ${OPENSSL_CONF} || exit 1

[ -f ${U2_CLIENT_KEY} ] || openssl genrsa -out ${U2_CLIENT_KEY} 2048 || exit 1
[ -f ${U2_CLIENT_CSR} ] || openssl req -new -key ${U2_CLIENT_KEY} -subj "/CN=u2" -out ${U2_CLIENT_CSR} -config ${OPENSSL_CONF} || exit 1
[ -f ${U2_CLIENT_CRT} ] || openssl x509 -req -in ${U2_CLIENT_CSR} -CA ${CA_CRT} -CAkey ${CA_KEY} -CAcreateserial -out ${U2_CLIENT_CRT} -days ${DAYS} -extensions v3_req  -extfile ${OPENSSL_CONF} || exit 1
