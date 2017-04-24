#!/usr/bin/env bash

if [ -z "$1" ]; then
	echo usage $0 MySQL_Password
	exit 1
fi
set -x
cd ..
cd shell
echo "####Now, running benchmark"
echo 3 > /proc/sys/vm/drop_caches
if [ -z "$BMI2" ]; then
	BMI2=1
fi
export LD_LIBRARY_PATH=/opt/${CXX}/lib64
if [ -d ../lib ]; then
	export LD_LIBRARY_PATH=../lib:$LD_LIBRARY_PATH
else
	export LD_LIBRARY_PATH=`cd ..; pwd`/pkg/terarkdb-tests-Linux-x86_64-${CXX}-bmi2-${BMI2}/lib:$LD_LIBRARY_PATH
fi
export DictZipBlobStore_zipThreads=24
./Terark_Engine_Test \
  terocksdb \
  --action=load \
  --alt_engine_name=terocksdb_load \
  --load_data_path=<(zcat /data/tpch_data/lineitem_512b.tbl.gz) \
  --db=/newssd1/terocksdb_tpch:400G,/newssd2/terocksdb_tpch:400G,/experiment/terocksdb_tpch:300G,/datainssd/terocksdb_tpch:300G \
  --logdir=/data/terocksdb_tpch.logdir \
  --waldir=/data/terocksdb_tpch.waldir \
  --terocksdb_tmpdir=/newssd2/terocksdb_tpch.tmpdir \
  --rocksdb_memtable=vector \
  --write_rate_limit=0 \
  --auto_slowdown_write=0 \
  --enable_auto_compact=0 \
  --load_size=10G \
  --write_buffer_size=200M \
  --zip_work_mem_soft_limit=1600M \
  --zip_work_mem_hard_limit=3000M \
  --small_task_mem=1400M \
  --fields_delim="|" \
  --fields_num=16 \
  --key_fields=0,1,2 \
  --disable_wal \
  --num_levels=4 \
  --index_nest_level=2 \
  --compact_threads=1 \
  --use_universal_compaction=1 \
  --target_file_size_multiplier=10 \
  --mysql_passwd=$1

