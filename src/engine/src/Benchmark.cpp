//
// Created by terark on 16-8-12.
//
#include "Benchmark.h"
tbb::concurrent_queue<std::string> Benchmark::updateDataCq;
fstrvec Benchmark::allkeys;
tbb::spin_rw_mutex Benchmark::allkeysRwMutex;
void Benchmark::updatePlan(std::vector<uint8_t> &plan, std::vector<std::pair<uint8_t ,uint8_t >> percent,uint8_t defaultVal){
    if (plan.size() < 100)
        plan.resize(100);
    uint8_t pos = 0;
    for(auto& eachPercent : percent){
        if (pos >= plan.size())
            break;
        std::fill_n(plan.begin() + pos,eachPercent.second,eachPercent.first);
        pos += eachPercent.second;
    }
    std::fill_n(plan.begin()+pos,100-pos,defaultVal);
    shufflePlan(plan);
}

void Benchmark::shufflePlan(std::vector<uint8_t > &plan){
    std::shuffle(plan.begin(),plan.end(),std::default_random_engine());
}
void Benchmark::adjustThreadNum(uint32_t target, std::atomic<std::vector<uint8_t >*>* whichEPlan,
                     std::atomic<std::vector<uint8_t >*>* whichSPlan){

    while (target > threads.size()){
        //Add new thread.
        ThreadState* state = newThreadState(whichEPlan,whichSPlan);
        threads.push_back( std::make_pair(std::thread(Benchmark::ThreadBody,this,state),state));
    }
    while (target < threads.size()){
        //delete thread
        threads.back().second->STOP.store(true);
        threads.back().first.join();
        delete threads.back().second;
        threads.pop_back();
    }
}
void Benchmark::adjustExecutePlan(uint8_t readPercent,uint8_t insertPercent){

    std::vector<std::pair<uint8_t ,uint8_t >> planDetails;
    planDetails.push_back(std::make_pair(1,readPercent));
    planDetails.push_back(std::make_pair(2,insertPercent));

    updatePlan(executePlan[whichEPlan],planDetails,0);
    executePlanAddr.store( &(executePlan[whichEPlan]));
    whichEPlan = !whichEPlan;
}
void Benchmark::adjustSamplingPlan(uint8_t samplingRate){
    std::vector<std::pair<uint8_t ,uint8_t >> planDetails;
    planDetails.push_back(std::make_pair(1,samplingRate));
    updatePlan(samplingPlan[whichSPlan],planDetails,0);
    samplingPlanAddr.store(& (samplingPlan[whichSPlan]));
    whichSPlan = !whichSPlan;
}
void Benchmark::RunBenchmark(void){
    int old_readPercent = -1;
    int old_samplingRate = -1;
    int old_insertPercent = -1;

    std::thread loadInsertDataThread(Benchmark::loadInsertData,&setting);
    while (!setting.ifStop()){
        int readPercent = setting.getReadPercent();
        int insertPercent = setting.getInsertPercent();
        if (readPercent != old_readPercent || old_insertPercent != insertPercent){
            //两份执行计划相互切换并不是完全的线程安全，
            //这里假定在经过5秒睡眠后，所有的其他线程都已经切换到了正确的执行计划。
            old_readPercent = readPercent;
            old_insertPercent = insertPercent;
            adjustExecutePlan(readPercent,insertPercent);
        }else{
            if (std::count(executePlan[whichEPlan].begin(),executePlan[whichEPlan].end(),1) != readPercent)
                executePlan[whichEPlan] = executePlan[!whichEPlan];
            shufflePlan(executePlan[whichEPlan]);
            executePlanAddr.store(&( executePlan[whichEPlan]));
            whichEPlan  = !whichEPlan;
        }
        int samplingRate = setting.getSamplingRate();
        if ( old_samplingRate != samplingRate){
            old_samplingRate = samplingRate;
            adjustSamplingPlan(samplingRate);
        }
        int threadNum = setting.getThreadNums();
        adjustThreadNum(threadNum,&executePlanAddr,&samplingPlanAddr);
        if (setting.getCompactTimes() > compactTimes){
            compactTimes++;
            std::thread compactThread(Benchmark::CompactThreadBody,this);
        }
        sleep(5);
    }
    adjustThreadNum(0, nullptr, nullptr);
    loadInsertDataThread.join();
}
bool Benchmark::executeOneOperationWithSampling(ThreadState* state,uint8_t type){
    struct timespec start,end;
    bool ret;
    clock_gettime(CLOCK_REALTIME,&start);
    if (ret = ((this->*executeFuncMap[type])(state))){
        clock_gettime(CLOCK_REALTIME,&end);
        state->stats.FinishedSingleOp(type, &start, &end);
    }
    return ret;
}
bool Benchmark::executeOneOperationWithoutSampling(ThreadState* state,uint8_t type){
    return ((this->*executeFuncMap[type])(state));
}
bool Benchmark::executeOneOperation(ThreadState* state,uint8_t type){
    assert(executeFuncMap.count(type) > 0);
    std::vector<uint8_t > *samplingPlan = (*(state->whichSamplingPlan)).load();
    samplingRecord[type] ++;
    if (samplingRecord[type] > 100){
        samplingRecord[type] = 0;
    }
    return (this->*samplingFuncMap[( (*samplingPlan)[(samplingRecord[type]-1) % samplingPlan->size()])])(state,type);
}
void Benchmark::ReadWhileWriting(ThreadState *thread) {
    std::cout << "Thread " << thread->tid << " start!" << std::endl;
    struct timespec start,end;
    while (thread->STOP.load() == false) {
        std::vector<uint8_t > *executePlan = (*(thread->whichExecutePlan)).load();
        for (auto type : *executePlan) {

            executeOneOperation(thread,type);
        }
    }
    std::cout << "Thread " << thread->tid << " stop!" << std::endl;
}

size_t Benchmark::updateKeys(void) {
    std::cout << "Update Keys:" << setting.getKeysDataPath() << std::endl;
    std::ifstream keysFile(setting.getKeysDataPath());
    assert(keysFile.is_open());
    std::string str;
    while( getline(keysFile,str)){
        if (str.size() == 0)
            continue;
        allkeys.push_back(str);
    }
    keysFile.close();
    return allkeys.size();
}

void Benchmark::backupKeys(void) {
    std::cout <<"backupKeys" << std::endl;
    std::fstream keyFile_bkup(setting.getKeysDataPath(),std::ios_base::trunc | std::ios_base::out);
    for( size_t i = 0; i < allkeys.size();++i){
        keyFile_bkup << allkeys.str(i) <<std::endl;
    }
    keyFile_bkup.close();
    std::cout <<"backupKeys finish" << std::endl;

}

bool Benchmark::getRandomKey(std::string &key,std::mt19937 &rg) {
    tbb::spin_rw_mutex::scoped_lock _smtx(allkeysRwMutex, false);//read lock
    if (allkeys.empty()){
        return false;
    }
    key.assign(allkeys.str(rg() % allkeys.size()));
    return true;
}

bool Benchmark::pushKey(std::string &key) {
    tbb::spin_rw_mutex::scoped_lock _smtx(allkeysRwMutex, true);//write lock
    try {
        allkeys.push_back(key);
    }catch (std::exception e){
        fprintf(stderr,"%s\n",e.what());
        return false;
    }
    return true;
}
void Benchmark::loadInsertData(const Setting *setting){
    FILE *ifs = fopen(setting->getInsertDataPath().c_str(),"r");
    assert(ifs != NULL);
    posix_fadvise(fileno(ifs),0,0,POSIX_FADV_SEQUENTIAL);
    std::string str;
    std::cout << "loadInsertData start" << std::endl;
    int count = 0;
    //LineBuf line;
    char *buf = NULL;
    size_t n = 0;
    while (setting->ifStop() == false && !feof(ifs)) {
        while( updateDataCq.unsafe_size() < 200000){
            if (feof(ifs))
                break;
            getline(&buf,&n,ifs);
            str = buf;
            updateDataCq.push(str);
            count ++;
        }
        if (n > 1024*1024) {
            n = 0;
            free(buf);
            buf = NULL;
        }
        std::cout << "insert : " << count << std::endl;
        count = 0;
        sleep(3);
    }
    fclose(ifs);
    std::cout << "loadInsertData stop" << std::endl;
    if (NULL != buf){
        free(buf);
        buf = NULL;
    }
}
void  Benchmark::Run(void){
    Open();
    std::cout << setting.ifRunOrLoad() << std::endl;
    if (setting.ifRunOrLoad() == "load") {
        allkeys.erase_all();
        Load();
        backupKeys();
    }
    else {
        std::cout << "allKeys size:" <<updateKeys() << std::endl;
        RunBenchmark();
    }
    Close();
};
Benchmark::Benchmark(const Setting &s):setting(s){
        compactTimes = 0;
        executeFuncMap[1] = &Benchmark::ReadOneKey;
        executeFuncMap[0] = &Benchmark::UpdateOneKey;
        executeFuncMap[2] = &Benchmark::InsertOneKey;
        samplingFuncMap[0] = &Benchmark::executeOneOperationWithoutSampling;
        samplingFuncMap[1] = &Benchmark::executeOneOperationWithSampling;
};
