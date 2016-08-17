//
// Created by terark on 16-8-12.
//
#include "Benchmark.h"
tbb::concurrent_queue<std::string> Benchmark::updateDataCq;
tbb::concurrent_vector<std::string> Benchmark::allkeys;
void Benchmark::updatePlan(std::vector<uint8_t> &plan, std::vector<std::pair<uint8_t ,uint8_t >> percent,uint8_t defaultVal){

    if (plan.size() < 100)
        plan.resize(100);
    uint8_t pos = 0;
    for(auto& eachPercent : percent){
        assert(pos <= 100);
        std::fill_n(plan.begin() + pos,eachPercent.second,eachPercent.first);
        pos += eachPercent.second;
    }
    assert(pos <= 100);
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

    std::thread loadInsertDataThread(Benchmark::loadInsertData,&insertFile,&setting);
    while( !setting.ifStop()){

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
    //return (this->*samplingFuncMap[( (*samplingPlan)[(samplingRecord[type]-1) % samplingPlan->size()])])(state,type);
    return executeOneOperationWithSampling(state,type);
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

std::string Benchmark::GatherTimeData(){
    std::stringstream ret;
    for( auto& eachThread : threads){
        ret << "Thread " << Stats::readTimeDataCq.unsafe_size() << std::endl;
    }
    return ret.str();
}

size_t Benchmark::updateKeys(void) {

    std::string str;
    while( getline(keysFile,str)){
        allkeys.push_back(str);
    }
    return allkeys.size();
}

void Benchmark::backupKeys(void) {
    std::cout <<"backupKeys" << std::endl;
    keysFile.close();
    std::fstream keyFile_bkup(setting.getKeysDataPath(),std::ios_base::trunc | std::ios_base::out);
    for(auto& eachKey : allkeys){
        keyFile_bkup << eachKey << std::endl;
    }
    keyFile_bkup.close();
    std::cout <<"backupKeys finish" << std::endl;

}
