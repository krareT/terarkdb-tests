
file=~/Documents/data/wiki.txt.gz

record_num=7911684
#cachesize=1073741824
cachesize=536870912
#cachesize=268435456
#cachesize=134217728
dirname=./experiment/wiki_wt
ratio=95
keys=~/Documents/data/wiki_keys
cd ../
cmake ./CMakeLists.txt
make
cd shell
echo "####Now, running wiredtiger benchmark"
zcat $file | ../build/Terark_Engine_Test wiredtiger --keys_data=$keys --benchmarks=fillrandom --num=$record_num --db=$dirname --use_lsm=0 --resource_data=/dev/stdin --threads=8
echo "####wiredtiger benchmark finish"


