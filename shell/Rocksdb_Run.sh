dirname=./experiment/rocksdb
keys_file=~/Documents/data/rocksdb_keys
insert_file=/mnt/hdd/data/xab
cd ../
cmake ./CMakeLists.txt
make
cd shell
echo "####Now, running wiredtiger benchmark"
cachesize=17179869184
export MYSQL_PASSWD=$1
../build/Terark_Engine_Test rocksdb -load_or_run=run -keys_data_path=$keys_file --db=$dirname --use_lsm=0 -insert_data_path=$insert_file --cache_size=$cachesize -read_percent=90 -insert_percent=5 -thread_num=8 1>/mnt/hdd/log/rocksdb_stdout_log 2>/mnt/hdd/log/rocksdb_stderr_log &
