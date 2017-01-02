#!/usr/bin/env bash

if [ -z "$1" ]; then
	echo usage $0 MySQL_Password
	exit 1
fi
set -x
cd ..
cmake ./CMakeLists.txt
make -j32
cd shell
echo "####Now, running benchmark"
echo 3 > /proc/sys/vm/drop_caches
export LD_LIBRARAY_PATH=/opt/gcc-5.4/lib64:/data/software/terark-Linux-x86_64-g++-5.4-bmi2-1/lib:/data/software/terark-db-Linux-x86_64-g++-5.4-bmi2-1/lib:/data/software/terark-zip-rocksdb-Linux-x86_64-g++-5.4-bmi2-1/lib
export DictZipBlobStore_zipThreads=12
../build/Terark_Engine_Test \
  terocksdb \
  --action=run \
  --keys_data_path=/disk2/tmp/lineitem.keys.0.06 \
  --insert_data_path=<(zcat /data/tpch_data/lineitem_512b.tbl.gz) \
  --db=/newssd1/terocksdb_tpch:400G,/newssd2/terocksdb_tpch:400G,/experiment/terocksdb_tpch:300G,/datainssd/terocksdb_tpch:300G \
  --logdir=/data/terocksdb_tpch.logdir \
  --waldir=/data/terocksdb_tpch.waldir \
  --terocksdb_tmpdir=/newssd2/terocksdb_tpch.tmpdir \
  --fields_delim="|" \
  --fields_num=16 \
  --key_fields=0,1,2 \
  --key_sample_ratio=0.05 \
  --disable_wal \
  --num_levels=4 \
  --compact_threads=2 \
  --universal_compaction=1 \
  --target_file_size_multiplier=5 \
  --plan_config=0:0:100:0 \
  --plan_config=1:100:0:0 \
  --thread_plan_map=0:0 \
  --thread_plan_map=1-24:1 \
  --thread_num=25 \
  --mysql_passwd=$1
