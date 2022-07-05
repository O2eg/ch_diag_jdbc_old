# ch_diag_jdbc_old
ClickHouse diagnostic tool for old versions


# ch_diag
ClickHouse 20.3+ diagnostic tool


```
# Connect using root CA certificate
java -jar ch_diag_jdbc_old.jar \
	--database=db1 \
	--host=rc1c-00lzn.net \
	--user=user1 \
	--password=123456 \
	--ca-certs=InternalRootCA.crt \
	--port=8443 \
	--cluster-name=test_cluster

# Connect using keyfile and certfile
java -jar ch_diag_jdbc_old.jar \
	--database=db1 \
	--host=rc1c-00lzn.net \
	--user=user1 \
	--password=123456 \
	--keyfile=node_01.key \
	--certfile=node_01.crt

# Connect using user and password
java -jar ch_diag_jdbc_old.jar \
	--database=default \
	--host=127.0.0.1 \
	--user=default \
	--password=123456 \
	--port=8124
```
