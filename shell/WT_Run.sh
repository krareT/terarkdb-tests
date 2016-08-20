
dirname=./experiment/wiki_wt
keys_file=~/Documents/data/wiki_keys
insert_file=/mnt/hdd/data/xab
cd ../
cmake ./CMakeLists.txt
make
cd shell
echo "####Clear cache"
echo 3 > /proc/sys/vm/drop_caches
echo "####Now, running wiredtiger benchmark"

export MYSQL_PASSWD=$1
../build/Terark_Engine_Test wiredtiger -load_or_run=run -keys_data_path=$keys_file --db=$dirname --use_lsm=0 -insert_data_path=$insert_file -read_percent=33 -insert_percent=33 -thread_num=8
