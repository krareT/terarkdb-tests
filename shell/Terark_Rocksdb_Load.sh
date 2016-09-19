load_file=/mnt/hdd/data/xaa
dirname=./experiment/terark_rocksdb
keys_file=~/Documents/data/terark_rocksdb_keys
cd ../
cmake ./CMakeLists.txt
make
cd shell
echo "####Now, running wiredtiger benchmark"
cachesize=17179869184
export MYSQL_PASSWD=$1
export TerRocksdb_Tmpdir=/mnt/hdd/TerRocksdb_Tmp
mkdir $TerRocksdb_Tmpdir
rm -rf $dirname/*
gdb --args ../build/Terark_Engine_Test terark_rocksdb -load_or_run=load -keys_data_path=$keys_file --db=$dirname --use_lsm=0 -load_data_path=$load_file --cache_size=$cachesize
