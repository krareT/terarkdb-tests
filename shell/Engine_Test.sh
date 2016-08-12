
file=~/Documents/data/wiki.txt.gz
key=~/Documents/data/wiki_keys
record_num=45
dirname=./experiment/wiki

cd ../
cmake CMakeLists.txt
make
cd shell
#rm -rf $dirname/*
export TMPDIR=$dirname
export DictZipBlobStore_zipThreads=8
echo $TMPDIR
cp ../Schema/dbmeta_wikipedia_index_int.json $dirname/dbmeta.json
echo "####Now, running terarkdb benchmark"
echo 3 > /proc/sys/vm/drop_caches
free -m
date
export TerarkDB_WrSegCacheSizeMB=500

../build/Terark_Engine_Test Terark --benchmarks=fillrandom --num=$record_num --sync_index=1 --db=$dirname --resource_data=/dev/stdin --threads=1 --keys_data=$key
free -m
date
du -s -b $dirname
echo "####terarkdb benchmark finish"
