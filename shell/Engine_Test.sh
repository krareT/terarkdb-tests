
file=~/Documents/data/movies.txt
record_num=7911684
read_num=40000000
dirname=./experiment/movies
ratio=95

cd ../
cmake CMakeLists.txt
make
cd shell
rm -rf $dirname/*
export TMPDIR=$dirname
export DictZipBlobStore_zipThreads=8
echo $TMPDIR
cp ../Schema/dbmeta_movies_index.json $dirname/dbmeta.json
echo "####Now, running terarkdb benchmark"
echo 3 > /proc/sys/vm/drop_caches
free -m
date
export TerarkDB_WrSegCacheSizeMB=100
../build/Terark_Engine_Test Terark --benchmarks=fillrandom --num=$record_num --sync_index=1 --db=$dirname --resource_data=$file --threads=8
free -m
date
du -s -b $dirname
echo "####terarkdb benchmark finish"
