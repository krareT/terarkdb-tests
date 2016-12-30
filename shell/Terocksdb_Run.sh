#!/usr/bin/env bash

if [ -z "$1" ]; then
	echo usage $0 MySQL_Password
	exit 1
fi
set -x
cd ../
cmake ./CMakeLists.txt
make -j32
cd shell
echo "####Now, running benchmark"
echo 3 > /proc/sys/vm/drop_caches
export DictZipBlobStore_zipThreads=8
../build/Terark_Engine_Test \
	terocksdb \
	--action=run \
    --db=/newssd1/terark_rocksdb \
	--keys_data_path=/disk2/tmp/wiki-keys \
	--insert_data_path=/data/publicdata/wikipedia/wikipedia.txt \
    --numfields=8 \
	--keyfields=2,7 \
    --cache_size=16G \
	--plan_config=0:90:10:0 \
	--thread_num=25 \
	--mysql_passwd=$1
