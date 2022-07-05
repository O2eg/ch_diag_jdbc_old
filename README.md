# ch_diag_jdbc_old
ClickHouse diagnostic tool for old versions


```
# Connect using root CA certificate
java -jar ch_diag_jdbc_old.jar \
	--host=rc1c-00lzn.net \
	--port=8443 \
	--database=db1 \
	--user=user1 \
	--password=123456 \
	--ca-certs=InternalRootCA.crt \
	--cluster-name=test_cluster

# Connect using keyfile and certfile
java -jar ch_diag_jdbc_old.jar \
	--host=rc1c-00lzn.net \
	--port=8443 \
	--database=db1 \
	--user=user1 \
	--password=123456 \
	--keyfile=node_01.key \
	--certfile=node_01.crt

# Connect using user and password
java -jar ch_diag_jdbc_old.jar \
	--host=127.0.0.1 \
	--port=8124 \
	--database=default \
	--user=default \
	--password=123456
```
