# Read Replica Demonstration Program

Running:

```bash
export YDB_SSL_ROOT_CERTIFICATES_FILE=/home/demo/rnd-ydb-ca.crt
export YDB_USER=tpcab
export YDB_PASSWORD=...

./read-replica-demo dump-keys -ydb 'grpcs://rnd-ydb10.front.private:2135/rnd-ydb/database' -out keys.dat --page-size 20000
```
