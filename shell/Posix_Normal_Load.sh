load_file=/data/publicdata/wikipedia/datahandle/wiki_30
dirname=/data/publicdata/wikipedia/experiment/posix_normal
cd ../
cmake CMakeLists.txt
make
cd shell
sudo rm -rf $dirname/*
echo "####Now, running posix benchmark"
echo 3 > /proc/sys/vm/drop_caches
date
export MYSQL_PASSWD=$1
sudo gdb --args ../build/Terark_Engine_Test posix -load_or_run=load --db=$dirname -load_data_path=$load_file
date
echo "####terarkdb posix finish"
