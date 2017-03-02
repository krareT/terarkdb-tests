keys_file=~/Documents/data/wiki_keys_terark
insert_file=/mnt/hdd/data/xab
dirname=./experiment/terark_wiki/
cd ../
cmake CMakeLists.txt
make
cd shell
export TMPDIR=$dirname
export DictZipBlobStore_zipThreads=8
echo $TMPDIR
cp ../Schema/dbmeta_wikipedia_index_int.json $dirname/dbmeta.json
echo "####Now, running terarkdb benchmark"
echo 3 > /proc/sys/vm/drop_caches
date
export TerarkDB_WrSegCacheSizeMB=500
export MYSQL_PASSWD=$1
../build/Terark_Engine_Test compact -read_percent=100 -insert_percent=0 -load_or_run=run -insert_data_path=$insert_file --sync_index=1 --db=$dirname --threads=8 -keys_data_path=$keys_file
date
echo "####terarkdb benchmark finish"
