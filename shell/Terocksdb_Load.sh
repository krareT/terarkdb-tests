#!/usr/bin/env bash

if [ -z "$1" ]; then
	echo usage $0 MySQL_Password
	exit 1
fi
set -x
dbdir=/newssd1/terark_rocksdb
cd ../
cmake ./CMakeLists.txt
make
cd shell
echo "####Now, running benchmark"
rm -rf $dbdir/*
../build/Terark_Engine_Test \
	terark_rocksdb \
	--load_or_run=load \
	--keys_data_path=~/Documents/data/terark_rocksdb_keys \
    --db=$dbdir \
    --numfields=8 \
	--keyfields=2,7 \
    --load_data_path=/data/publicdata/wikipedia/wikipedia.txt \
	--cache_size=16G \
	--terocksdb_tmpdir=/newssd2/terark_rocksdb.tmpdir \
	--mysql_passwd=$1
