
load_file=/mnt/hdd/data/wiki_clean_load.txt.gz
keys_file=~/Documents/data/wiki_keys
insert_file=/mnt/hdd/data/wiki_clean_insert.txt.gz
dirname=./experiment/new_wiki/

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
zcat $insert_file |  ../build/Terark_Engine_Test terarkdb -load_or_run=run -insert_data_path=/dev/stdin --sync_index=1 --db=$dirname -load_data_path=/dev/stdin --threads=1 -keys_data_path=$keys_file
date
echo "####terarkdb benchmark finish"
