
load_file=/media/terark0/hdd/data/wiki_load.txt.gz
keys_file=~/Documents/data/wiki_keys
dirname=./experiment/terark_wiki/
cd ../
cmake CMakeLists.txt
make
cd shell
export TMPDIR=$dirname
export DictZipBlobStore_zipThreads=8
echo $TMPDIR
rm -rf $dirname/*
cp ../Schema/dbmeta_wikipedia_index_int.json $dirname/dbmeta.json
echo "####Now, running terarkdb benchmark"
echo 3 > /proc/sys/vm/drop_caches
date
export TerarkDB_WrSegCacheSizeMB=500
export MYSQL_PASSWD=$1
zcat $load_file | ../build/Terark_Engine_Test terarkdb -load_or_run=load --sync_index=1 --db=$dirname -keys_data_path=$keys_file -load_data_path=/dev/stdin
date
echo "####terarkdb benchmark finish"
