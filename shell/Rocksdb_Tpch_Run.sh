#!/usr/bin/env bash
set -x
insert_file=/data/tpch_data/lineitem_512b.tbl.gz
dirname=/newssd1/rocksdb_tpch:400G,/newssd2/rocksdb_tpch:400G,/experiment/rocksdb_tpch:300G,/datainssd/rocksdb_tpch:300G
keys_file=/disk2/tmp/lineitem.keys.0.06
cd ..
cmake ./CMakeLists.txt
make -j32
cd shell
echo "####Now, running benchmark"
echo 3 > /proc/sys/vm/drop_caches
export MYSQL_PASSWD=$1
../build/Terark_Engine_Test \
  rocksdb \
  -load_or_run=run \
  -keys_data_path=$keys_file \
  -insert_data_path=<(zcat $insert_file) \
  --db=$dirname \
  --logdir=/data/rocksdb_tpch.logdir \
  --waldir=/data/rocksdb_tpch.waldir \
  --fieldsDelim="|" \
  --numfields=16 \
  --keyfields=0,1,2 \
  --keySampleRatio=0.05 \
  --cache_size=16G \
  --disableWAL \
  --numLevels=4 \
  --universalCompaction=0 \
  --target_file_size_multiplier=1 \
  -plan_config=0:0:100:0 \
  -plan_config=1:100:0:0 \
  -thread_plan_map=0-1:0 \
  -thread_plan_map=2-24:1 \
  -thread_num=25


    #\
    #1>/disk2/tmp/terocks_stdout_log \
    #2>/disk2/tmp/terocks_stderr_log &
