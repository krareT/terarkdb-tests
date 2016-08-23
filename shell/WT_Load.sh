load_file=/mnt/hdd/data/wiki_load.txt.gz
dirname=./experiment/wiki_wt
keys_file=~/Documents/data/wiki_keys
cd ../
cmake ./CMakeLists.txt
make
cd shell
echo "####Now, running wiredtiger benchmark"
cachesize=17179869184
export MYSQL_PASSWD=$1
rm -rf $dirname/*
zcat $load_file | ../build/Terark_Engine_Test wiredtiger -load_or_run=load -keys_data_path=$keys_file --db=$dirname --use_lsm=0 -load_data_path=/dev/stdin --cache_size=$cachesize
