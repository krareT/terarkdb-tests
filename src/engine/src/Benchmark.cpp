//
// Created by terark on 16-8-12.
//
#include "Benchmark.h"
tbb::concurrent_queue<std::string> Benchmark::updateDataCq;
fstrvec Benchmark::allkeys;
tbb::spin_rw_mutex Benchmark::allkeysRwMutex;
void Benchmark::updateSamplingPlan(std::vector<bool> &plan, uint8_t percent) {

    if (percent > 100)
        return ;
    plan.resize(100);
    std::fill_n(plan.begin(),percent, true);
    std::shuffle(plan.begin(),plan.end(),std::default_random_engine());
}

void Benchmark::shufflePlan(std::vector<uint8_t > &plan){
    std::shuffle(plan.begin(),plan.end(),std::default_random_engine());
}
void Benchmark::adjustThreadNum(uint32_t target, std::atomic<std::vector<bool > *> *whichSPlan) {

    while (target > threads.size()){
        //Add new thread.
        ThreadState* state = newThreadState(whichSPlan);
        state->planConfig.read_percent = 100;
        state->planConfig.write_percent = 0;
        state->planConfig.update_percent = 0;
        updatePlan(state->planConfig,state->executePlan[0]);
        state->whichPlan.store(0,std::memory_order_relaxed);
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
void Benchmark::adjustSamplingPlan(uint8_t samplingRate){
    std::vector<std::pair<uint8_t ,uint8_t >> planDetails;
    planDetails.push_back(std::make_pair(1,samplingRate));
    updateSamplingPlan(samplingPlan[whichSPlan], samplingRate);
    samplingPlanAddr.store(& (samplingPlan[whichSPlan]));
    whichSPlan = !whichSPlan;
}
void Benchmark::RunBenchmark(void){

    int old_samplingRate = -1;

    std::thread loadInsertDataThread(Benchmark::loadInsertData,&setting);

    while (!setting.ifStop()){

        //check sampling rate
        int samplingRate = setting.getSamplingRate();
        if ( old_samplingRate != samplingRate){
            old_samplingRate = samplingRate;
            adjustSamplingPlan(samplingRate);
        }
        //check thread num
        int threadNum = setting.getThreadNums();
        adjustThreadNum(threadNum, &samplingPlanAddr);
        //check execute plan
        checkExecutePlan();
        //check compact
        auto ct = setting.getCompactTimes();
        if ( ct != compactTimes){
            compactTimes = ct;
            std::thread compactThread(Benchmark::CompactThreadBody,this);
            compactThread.detach();
        }
        //check message
        auto handle_message_times = 5;
        while (handle_message_times--)
            reportMessage(HandleMessage(setting.getMessage()));
        sleep(5);
    }
    adjustThreadNum(0, nullptr);
    loadInsertDataThread.join();
}
bool Benchmark::executeOneOperationWithSampling(ThreadState *state, BaseSetting::OP_TYPE type){
    struct timespec start,end;
    bool ret;
    clock_gettime(CLOCK_REALTIME,&start);
    ret = (this->*executeFuncMap[type])(state);
    if (ret == true || type == BaseSetting::OP_TYPE::READ) {
        clock_gettime(CLOCK_REALTIME, &end);
        state->stats.FinishedSingleOp(type, &start, &end);
    }
    return ret;
}
bool Benchmark::executeOneOperationWithoutSampling(ThreadState *state, BaseSetting::OP_TYPE type){
    return ((this->*executeFuncMap[type])(state));
}
bool Benchmark::executeOneOperation(ThreadState* state,BaseSetting::OP_TYPE type){

    assert(executeFuncMap.count(type) > 0);
    std::vector<bool > *samplingPlan = (*(state->whichSamplingPlan)).load();

    if (samplingRecord[type] >= samplingPlan->size()){
        samplingRecord[type] = 0;
    }
    bool ifSampling = (*samplingPlan)[samplingRecord[type]] ;
    samplingRecord[type] ++;
    return (this->*samplingFuncMap[ifSampling])(state,type);
}
void Benchmark::ReadWhileWriting(ThreadState *thread) {
    std::cout << "Thread " << thread->tid << " start!" << std::endl;

    while (thread->STOP.load() == false) {
        const auto &executePlan = thread->executePlan[thread->whichPlan.load(std::memory_order_relaxed)];
        for (auto type : executePlan) {
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
    if (allkeys.size() == 0)
        return;
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
	std::cout << setting->getInsertDataPath() << std::endl;
	if ( ifs == NULL)
		puts(strerror(errno));    
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
    if (setting.ifRunOrLoad() == "load") {
        try {
            std::cout << "load" << std::endl;
            allkeys.erase_all();
            Load();
            backupKeys();
        } catch (const std::exception &e) {
            std::cout << e.what() << std::endl;
        }
    }
    else {
        std::cout << "allKeys size:" <<updateKeys() << std::endl;
        RunBenchmark();
    }
    Close();
};

Benchmark::Benchmark(Setting &s) : setting(s) {
    compactTimes = s.getCompactTimes();
        executeFuncMap[BaseSetting::OP_TYPE::READ] = &Benchmark::ReadOneKey;
        executeFuncMap[BaseSetting::OP_TYPE::UPDATE] = &Benchmark::UpdateOneKey;
        executeFuncMap[BaseSetting::OP_TYPE::INSERT] = &Benchmark::InsertOneKey;
        samplingFuncMap[false] = &Benchmark::executeOneOperationWithoutSampling;
        samplingFuncMap[true] = &Benchmark::executeOneOperationWithSampling;
}

std::string Benchmark::HandleMessage(const std::string &msg) {
	std::string empty_str;
	return empty_str;
}

void Benchmark::reportMessage(const std::string &msg) {
    if (msg.empty())
        return;
    setting.sendMessageToSetting(msg);
}

void Benchmark::updatePlan(const PlanConfig &pc, std::vector<BaseSetting::OP_TYPE>& plan) {

    uint32_t total_size = pc.update_percent + pc.read_percent + pc.write_percent;
    plan.resize(total_size);
    fill_n(plan.begin(),pc.read_percent,BaseSetting::OP_TYPE::READ);
    fill_n(plan.begin() + pc.read_percent,pc.write_percent,BaseSetting::OP_TYPE::INSERT);
    fill_n(plan.begin() + pc.read_percent + pc.write_percent,pc.update_percent,BaseSetting::OP_TYPE::UPDATE);
    std::shuffle(plan.begin(),plan.end(),std::default_random_engine());
}

void Benchmark::checkExecutePlan() {
    PlanConfig pc;
    for( auto &eachThread : threads){
        setting.getPlanConfig(eachThread.second->tid,pc);
        auto & threadPc = eachThread.second->planConfig;
        if ( pc.read_percent == threadPc.read_percent
             && pc.update_percent == threadPc.update_percent
             && pc.write_percent == threadPc.write_percent) {
            continue;
        }
        threadPc = pc;
        auto which = eachThread.second->whichPlan.load(std::memory_order_relaxed);
        updatePlan(threadPc,eachThread.second->executePlan[ !which]);
        eachThread.second->whichPlan.store(!which);
    }
};
