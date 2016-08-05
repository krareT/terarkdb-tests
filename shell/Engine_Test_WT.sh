
file=~/Documents/data/movies.txt

record_num=7911684
read_num=40000000
#cachesize=1073741824
cachesize=536870912
#cachesize=268435456
#cachesize=134217728
dirname=./experiment/movies
ratio=95

cd ../
cmake ./CMakeLists.txt
make
cd shell
rm -rf $dirname/*
echo "####Now, running wiredtiger benchmark"
../build/Terark_Engine_Test WiredTiger --benchmarks=fillrandom --num=$record_num --db=$dirname --use_lsm=0 --resource_data=$file --threads=8
echo "####wiredtiger benchmark finish"

echo "####Now, running wiredtiger benchmark"
echo 3 > /proc/sys/vm/drop_caches
free -m
date

