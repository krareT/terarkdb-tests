//
// Created by terark on 9/10/16.
//

#include <dirent.h>
#include "PosixBenchmark.h"
PosixBenchmark::PosixBenchmark(Setting &setting):Benchmark(setting){

}

void PosixBenchmark::Open(void) {

    DIR *dir;
    dir = opendir(setting.FLAGS_db);
    if (dir == NULL){
        throw std::invalid_argument(strerror(errno));
    }
    auto ret = closedir(dir);
    if ( ret != 0){
        throw std::invalid_argument(strerror(errno));
    }
}

void PosixBenchmark::Close(void) {
}

void PosixBenchmark::Load(void) {
    DIR *dir;
    std::cout << "PosixBenchmakr::Load to :" + setting.getLoadDataPath() << std::endl;
    dir = opendir(setting.getLoadDataPath().c_str());
    if (dir == NULL){
        throw std::invalid_argument(strerror(errno) + setting.getLoadDataPath());
    }
    int ret = closedir(dir);
    if (ret != 0){
        throw std::invalid_argument(strerror(errno) + setting.getLoadDataPath());
    }
    if (system(NULL) == 0)
        throw std::logic_error("system error:no bash to use!\n");

    std::string cp_cmd = "cp " + setting.getLoadDataPath() + " " + setting.FLAGS_db + "/" + " -r";
    std::cout << cp_cmd << std::endl;
    ret = system(cp_cmd.c_str());
    if (ret == -1)
        throw std::runtime_error("cp_cmd execute failed,child ,can't create child process.\n");
    if (ret == 127)
        throw std::runtime_error("cp_cmd execute failed!\n");
    return ;
}

bool PosixBenchmark::ReadOneKey(ThreadState *ts) {
    if (false == getRandomKey(ts->key, ts->randGenerator))
        return false;
    std::string read_path = setting.FLAGS_db;
    read_path = read_path + "/" + ts->key;
    std::unique_ptr<FILE, decltype(&fclose)> read_file(fopen(read_path.c_str(),"r"),fclose);
    if (read_file.get() == nullptr)
        return false;
    auto size = ftell(read_file.get());

    std::unique_ptr<char > buf(new char [size]);
    if (size != fread(buf.get(),1,size,read_file.get())){
        fprintf(stderr,"posix read error:%s\n",strerror(errno));
        return false;
    }
    return true;
}

bool PosixBenchmark::UpdateOneKey(ThreadState *ts) {
    if (false == getRandomKey(ts->key, ts->randGenerator)){
        fprintf(stderr,"RocksDbBenchmark::UpdateOneKey:getRandomKey false\n");
        return false;
    }
    ts->key = setting.FLAGS_db + ts->key;
    std::unique_ptr<FILE, decltype(&fclose)> update_file(fopen(ts->key.c_str(),"r"),fclose);
    auto size = ftell(update_file.get());
    std::unique_ptr<char> buf(new char[size]);
    if ( size != fread(buf.get(),1,size,update_file.get())){
        fprintf(stderr,"posix update read error:%s\n",strerror(errno));
        return false;
    }
    if ( size != fwrite(buf.get(),1,size,update_file.get())){
        fprintf(stderr,"posix update write error:%s\n",strerror(errno));
        return false;
    }
    return true;
}

bool PosixBenchmark::InsertOneKey(ThreadState *ts) {
    if (updateDataCq.try_pop(ts->key) == false) {
        return false;
    }
    ts->str = setting.getInsertDataPath() + "/" + ts->key;
    std::unique_ptr<FILE, decltype(&fclose)> insert_source_file(fopen(ts->str.c_str(),"r"),fclose);
    ts->str = setting.FLAGS_db;
    ts->str = ts->str + "/" + ts->key;
    std::unique_ptr<FILE, decltype(&fclose)> insert_target_file(fopen(ts->str.c_str(),"w+"),fclose);
    auto size = ftell(insert_source_file.get());
    std::unique_ptr<char> buf(new char[size]);
    if (size != fread(buf.get(),1,size,insert_source_file.get())){
        fprintf(stderr,"posix insert read error:%s\n",strerror(errno));
        return false;
    }
    if (size != fwrite(buf.get(),1,size,insert_target_file.get())){
        fprintf(stderr,"posix insert write error:%s\n",strerror(errno));
        return false;
    }
    return true;
}

bool PosixBenchmark::Compact(void) {
    return false;
}

ThreadState *PosixBenchmark::newThreadState(std::atomic<std::vector<bool> *> *whichSamplingPlan) {
    return new ThreadState(threads.size(), nullptr,whichSamplingPlan);
}



