insert_file=/mnt/hdd/data/xab
dirname=./experiment/terark_rocksdb
keys_file=~/Documents/data/terark_rocksdb_keys
cd ../
cmake ./CMakeLists.txt
make
cd shell
echo "####Now, running wiredtiger benchmark"
cachesize=17179869184
echo 3 > /proc/sys/vm/drop_caches
export MYSQL_PASSWD=$1
export TerRocksdb_Tmpdir=/mnt/hdd/TerRocksdb_Tmp
export DictZipBlobStore_zipThreads=3
mkdir $TerRocksdb_Tmpdir
../build/Terark_Engine_Test terark_rocksdb -load_or_run=run -keys_data_path=$keys_file --db=$dirname --use_lsm=0 -insert_data_path=$insert_file --cache_size=$cachesize -thread_num=8 -read_percent=90 -insert_percent=5 1>/mnt/hdd/log/terocks_stdout_log 2>/mnt/hdd/log/terocks_stderr_log &
#../build/Terark_Engine_Test terark_rocksdb -load_or_run=run -keys_data_path=$keys_file --db=$dirname --use_lsm=0 -insert_data_path=$insert_file --cache_size=$cachesize -thread_num=8 -read_percent=90 -insert_percent=5 
