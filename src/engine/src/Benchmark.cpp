//
// Created by terark on 16-8-12.
//
#include "Benchmark.h"
#include <terark/util/autoclose.hpp>
#include <terark/util/linebuf.hpp>

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
        state->planConfig.insert_percent = 0;
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
    std::thread loadInsertDataThread(&Benchmark::loadInsertData, this, &setting);
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
    clock_gettime(CLOCK_REALTIME,&start);
    bool ret = (this->*executeFuncMap[int(type)])(state);
    if (ret || type == BaseSetting::OP_TYPE::READ) {
        clock_gettime(CLOCK_REALTIME, &end);
        state->stats.FinishedSingleOp(type, &start, &end);
    }
    return ret;
}

bool Benchmark::executeOneOperationWithoutSampling(ThreadState *state, BaseSetting::OP_TYPE type){
    return ((this->*executeFuncMap[int(type)])(state));
}

bool Benchmark::executeOneOperation(ThreadState* state,BaseSetting::OP_TYPE type){
    assert(executeFuncMap[int(type)] != 0);
    std::vector<bool> *samplingPlan = (*(state->whichSamplingPlan)).load();
    if (samplingRecord[int(type)] >= samplingPlan->size()){
        samplingRecord[int(type)] = 0;
    }
    bool ifSampling = (*samplingPlan)[samplingRecord[int(type)]] ;
    samplingRecord[int(type)]++;
    return (this->*samplingFuncMap[ifSampling])(state,type);
}

void Benchmark::ReadWhileWriting(ThreadState *thread) {
    std::cout << "Thread " << thread->tid << " start!" << std::endl;
    while (!thread->STOP.load()) {
        const auto &executePlan = thread->executePlan[thread->whichPlan.load(std::memory_order_relaxed)];
        for (auto type : executePlan) {
            executeOneOperation(thread,type);
        }
    }
    std::cout << "Thread " << thread->tid << " stop!" << std::endl;
}

void Benchmark::loadKeys(double keySampleRatio) {
    std::cout << "Load Keys: " << setting.getKeysDataPath() << std::endl;
	std::ifstream keysFile(setting.getKeysDataPath());
    assert(keysFile.is_open());
    std::string str;
    if (keySampleRatio < 0.99) {
        std::mt19937_64 random;
        size_t upperBound = size_t(random.max() * keySampleRatio);
        while (getline(keysFile, str)) {
            if (str.size() && random() < upperBound)
                allkeys.push_back(str);
        }
    }
    else {
        while (getline(keysFile, str)) {
            if (str.size())
                allkeys.push_back(str);
        }
    }
    keysFile.close();
}

void Benchmark::backupKeys(const std::string& fname) {
    std::cout <<"backupKeys: " << fname << std::endl;
    if (allkeys.size() == 0)
        return;
    std::fstream keyFile_bkup(fname,std::ios_base::trunc | std::ios_base::out);
    for( size_t i = 0; i < allkeys.size();++i){
        keyFile_bkup << allkeys.str(i) << std::endl;
    }
    keyFile_bkup.close();
    std::cout <<"backupKeys finished" << std::endl;
}

bool Benchmark::getRandomKey(std::string &key,std::mt19937_64 &rg) {
    if (allkeys.empty()){
        return false;
    }
    auto randomIndex = rg() % allkeys.size();
    tbb::spin_rw_mutex::scoped_lock _smtx(allkeysRwMutex, false);//read lock
    const char*  p = allkeys.beg_of(randomIndex);
    const size_t n = allkeys.slen(randomIndex);
    key.assign(p, n);
    return true;
}

bool Benchmark::pushKey(std::string &key) {
    if (setting.keySampleRatio > 0.90 ||
            random() < random.max() * setting.keySampleRatio) {
        tbb::spin_rw_mutex::scoped_lock _smtx(allkeysRwMutex, true);//write lock
        try {
            allkeys.push_back(key);
        } catch (const std::exception& e) {
            fprintf(stderr, "%s\n", e.what());
            return false;
        }
    }
    return true;
}

extern bool g_upload_fake_ops;

void Benchmark::loadInsertData(const Setting *setting){
    const char* fpath = setting->getInsertDataPath().c_str();
    Auto_fclose ifs(fopen(fpath, "r"));
	if (!ifs) {
        fprintf(stderr, "ERROR: fopen(%s, r) = %s\n", fpath, strerror(errno));
        return;
    }
    fprintf(stderr, "Benchmark::loadInsertData(%s) start\n", fpath);
    LineBuf line;
    size_t lines = 0;
    while (lines < setting->skipInsertLines && line.getline(ifs) > 0) {
        lines++;
    }
    fprintf(stderr, "Benchmark::loadInsertData(%s) skipped %zd lines\n", fpath, lines);
    while (!setting->ifStop() && !feof(ifs)) {
        size_t count = 0;
        while (updateDataCq.unsafe_size() < 200000 && line.getline(ifs) > 0) {
            updateDataCq.push(std::string(line.p, line.n));
            count++;
        }
        lines += count;
    //  printf("Benchmark::loadInsertData(): total = %9d   chunklines = %6d\n", lines, count);
        usleep(300000);
    }
    g_upload_fake_ops = true;
    fprintf(stderr, "Benchmark::loadInsertData(%s) %s, lines = %zd\n", fpath
        , setting->ifStop()? "stopped" : "completed", lines);
    if (!setting->ifStop()) {
        fprintf(stderr, "Benchmark::loadInsertData(): all data are loaded, wait for 2 minutes then compact!\n");
        usleep(2*60*1000*1000);
        fprintf(stderr, "Benchmark::loadInsertData(): start compact ...\n");
        Compact();
        fprintf(stderr, "Benchmark::loadInsertData(): compaction finished!\n");
    }
}

void Benchmark::Run(void) {
    Open();
    if (setting.ifRunOrLoad() == "load") {
        try {
            std::cout << "load" << std::endl;
            allkeys.erase_all();
            Load();
            backupKeys(setting.getKeysDataPath());
        } catch (const std::exception &e) {
            std::cout << e.what() << std::endl;
        }
    }
    else if (setting.ifRunOrLoad() == "compact") {
        this->Compact();
    }
    else {
        loadKeys(1.0);
    //  backupKeys(setting.getKeysDataPath() + ".bak"); // temporary
        std::cout << "allKeys size:" << allkeys.size() << std::endl;
        RunBenchmark();
    }
    Close();
};

Benchmark::Benchmark(Setting &s) : setting(s)
, executeFuncMap{&Benchmark::ReadOneKey,
                 &Benchmark::InsertOneKey,
                 &Benchmark::UpdateOneKey,
        }
, samplingFuncMap{&Benchmark::executeOneOperationWithoutSampling,
                  &Benchmark::executeOneOperationWithSampling,
        }
{
    compactTimes = s.getCompactTimes();
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
    uint32_t total_size = pc.update_percent + pc.read_percent + pc.insert_percent;
    plan.resize(total_size);
    fill_n(plan.begin(),pc.read_percent,BaseSetting::OP_TYPE::READ);
    fill_n(plan.begin() + pc.read_percent,pc.insert_percent,BaseSetting::OP_TYPE::INSERT);
    fill_n(plan.begin() + pc.read_percent + pc.insert_percent,pc.update_percent,BaseSetting::OP_TYPE::UPDATE);
    std::shuffle(plan.begin(),plan.end(),std::default_random_engine());
}

void Benchmark::checkExecutePlan() {
    PlanConfig pc;
    for( auto &eachThread : threads) {
        setting.getPlanConfig(eachThread.second->tid,pc);
        auto & threadPc = eachThread.second->planConfig;
        if ( pc.read_percent == threadPc.read_percent
             && pc.update_percent == threadPc.update_percent
             && pc.insert_percent == threadPc.insert_percent) {
            continue;
        }
        threadPc = pc;
        auto which = eachThread.second->whichPlan.load(std::memory_order_relaxed);
        updatePlan(threadPc,eachThread.second->executePlan[ !which]);
        eachThread.second->whichPlan.store(!which);
    }
};
