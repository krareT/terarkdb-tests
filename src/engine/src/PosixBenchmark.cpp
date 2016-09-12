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
    dir = opendir(setting.getLoadDataPath().c_str());
    if (dir == NULL){
        throw std::invalid_argument(strerror(errno));
    }
    auto ret = closedir(dir);
    if ( ret != 0){
        throw std::invalid_argument(strerror(errno));
    }

    if ( system(NULL) == 0)
        throw std::logic_error("system error:no bash to use!\n");

    std::string cp_cmd = "cp " + setting.getLoadDataPath() + "/./* " + setting.FLAGS_db + "/" + " -r";
    std::cout << cp_cmd << std::endl;
    ret = system(cp_cmd.c_str());
    if (ret == -1)
        throw std::runtime_error("cp_cmd execute failed,child ,can't create child process.\n");
    if (ret == 127)
        throw std::runtime_error("cp_cmd execute failed!\n");
    return ;
}

bool PosixBenchmark::ReadOneKey(ThreadState *) {
    return false;
}

bool PosixBenchmark::UpdateOneKey(ThreadState *) {
    return false;
}

bool PosixBenchmark::InsertOneKey(ThreadState *) {
    return false;
}

ThreadState *PosixBenchmark::newThreadState(std::atomic<std::vector<uint8_t> *> *whichExecutePlan,
                                            std::atomic<std::vector<uint8_t> *> *whichSamplingPlan) {
    return nullptr;
}

bool PosixBenchmark::Compact(void) {
    return false;
}



