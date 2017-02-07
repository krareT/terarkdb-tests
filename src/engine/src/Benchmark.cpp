//
// Created by terark on 16-8-12.
//
#include "Benchmark.h"
#include <terark/util/autoclose.hpp>
#include <terark/util/linebuf.hpp>
#include <terark/util/profiling.hpp>

using namespace terark;

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
        ThreadState* state = newThreadState(whichSPlan);
        state->planConfig.read_percent = 100;
        state->planConfig.insert_percent = 0;
        state->planConfig.update_percent = 0;
        updatePlan(state->planConfig,state->executePlan[0]);
        state->whichPlan.store(0,std::memory_order_relaxed);
        threads.emplace_back(std::thread([&](){ReadWhileWriting(state);}), state);
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
    std::thread loadInsertDataThread(&Benchmark::loadInsertData, this);
    while (!setting.ifStop()){
        int samplingRate = setting.getSamplingRate();
        if (old_samplingRate != samplingRate){
            old_samplingRate = samplingRate;
            adjustSamplingPlan(samplingRate);
        }
        int threadNum = setting.getThreadNums();
        adjustThreadNum(threadNum, &samplingPlanAddr);
        checkExecutePlan();
        auto ct = setting.getCompactTimes();
        if ( ct != compactTimes){
            compactTimes = ct;
            std::thread compactThread([&](){
              static std::mutex mtx;
              std::lock_guard<std::mutex> lock(mtx);
              this->Compact();
            });
            compactThread.detach();
        }
        auto handle_message_times = 5;
        while (handle_message_times--)
            reportMessage(HandleMessage(setting.getMessage()));
        sleep(5);
    }
    adjustThreadNum(0, nullptr);
    loadInsertDataThread.join();
}

bool Benchmark::executeOneOperationWithSampling(ThreadState *state, OP_TYPE type){
    struct timespec start,end;
    clock_gettime(CLOCK_REALTIME,&start);
    bool ret = (this->*executeFuncMap[int(type)])(state);
    if (ret || type == OP_TYPE::SEARCH) {
        clock_gettime(CLOCK_REALTIME, &end);
        state->stats.FinishedSingleOp(type, start, end);
    }
    return ret;
}

bool Benchmark::executeOneOperationWithoutSampling(ThreadState *state, OP_TYPE type){
    return ((this->*executeFuncMap[int(type)])(state));
}

bool Benchmark::executeOneOperation(ThreadState* state,OP_TYPE type){
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
    std::cout << "Thread " << thread->tid << " run Benchmark::ReadWhileWriting() start..." << std::endl;
    while (!thread->STOP.load()) {
        const auto &executePlan = thread->executePlan[thread->whichPlan.load(std::memory_order_relaxed)];
        for (auto type : executePlan) {
            executeOneOperation(thread,type);
        }
    }
    std::cout << "Thread " << thread->tid << " run Benchmark::ReadWhileWriting() exit" << std::endl;
}

void Benchmark::loadKeys() {
    std::cout << "Load Keys: " << setting.getKeysDataPath() << std::endl;
    std::ifstream keysFile(setting.getKeysDataPath());
    assert(keysFile.is_open());
    std::string str;
    while (std::getline(keysFile, str)) {
        if (str.size())
            allkeys.push_back(str);
    }
    allkeys.shrink_to_fit();
    keysFile.close();
    std::cout << "Load Keys: allkeys.size() = " << allkeys.size() << std::endl;
}

bool Benchmark::getRandomKey(std::string &key,std::mt19937_64 &rg) {
    if (allkeys.empty()){
        return false;
    }
    auto randomIndex = rg() % allkeys.size();
    static std::mutex mtx;
    std::unique_lock<std::mutex> lock(mtx);
    const char*  p = allkeys.beg_of(randomIndex);
    const size_t n = allkeys.slen(randomIndex);
    key.assign(p, n);
    return true;
}

extern bool g_upload_fake_ops;

void Benchmark::loadInsertData(){
    const char* fpath = setting.getInsertDataPath().c_str();
    Auto_fclose ifs(fopen(fpath, "r"));
    if (!ifs) {
        fprintf(stderr, "ERROR: fopen(%s, r) = %s\n", fpath, strerror(errno));
        return;
    }
    size_t limit = setting.FLAGS_load_size;
    fprintf(stderr, "Benchmark::loadInsertData(%s) start, limit = %f GB\n", fpath, limit/1e9);
    LineBuf line;
    size_t lines = 0;
    while (lines < setting.skipInsertLines && line.getline(ifs) > 0) {
        lines++;
    }
    fprintf(stderr, "Benchmark::loadInsertData(%s) skipped %zd lines\n", fpath, lines);
    terark::profiling pf;
    long long t0 = pf.now();
    size_t bytes = 0;
    while (bytes < limit && !setting.ifStop() && !feof(ifs)) {
        size_t count = 0;
        while (bytes < limit && updateDataCq.unsafe_size() < 200000 && line.getline(ifs) > 0) {
            line.chomp();
            bytes += line.size();
            updateDataCq.push(std::string(line.p, line.n));
            count++;
        }
        lines += count;
    //  printf("Benchmark::loadInsertData(): total = %9d   chunklines = %6d\n", lines, count);
        usleep(300000);
    }
    long long t1 = pf.now();
    g_upload_fake_ops = true;
    fprintf(stderr
        , "Benchmark::loadInsertData(%s) %s, lines = %zd, bytes = %zd, time = %f sec, speed = %f MB/sec\n"
        , fpath
        , setting.ifStop() ? "stopped" : "completed"
        , lines, bytes, pf.sf(t0,t1), bytes/pf.uf(t0,t1)
        );
    if (!setting.ifStop()) {
        fprintf(stderr, "Benchmark::loadInsertData(): all data are loaded, wait for %f sec then compact!\n", pf.sf(t0,t1)/2);
        usleep(pf.us(t0,t1)/2);
        fprintf(stderr, "Benchmark::loadInsertData(): start compact ...\n");
        Compact();
        fprintf(stderr, "Benchmark::loadInsertData(): compaction finished!\n");
    }
}

void Benchmark::Run(void) {
    Open();
    if (setting.getAction() == "load") {
        try {
            std::cout << "load" << std::endl;
            allkeys.erase_all();
            Load();
        } catch (const std::exception &e) {
            std::cout << e.what() << std::endl;
        }
    }
    else if (setting.getAction() == "compact") {
        this->Compact();
    }
    else {
        loadKeys();
        RunBenchmark();
    }
    Close();
};

Benchmark::Benchmark(Setting& s)
: executeFuncMap{&Benchmark::ReadOneKey,
                 &Benchmark::InsertOneKey,
                 &Benchmark::UpdateOneKey,
        }
, samplingFuncMap{&Benchmark::executeOneOperationWithoutSampling,
                  &Benchmark::executeOneOperationWithSampling,
        }
, setting(s)
{
    compactTimes = s.getCompactTimes();
}

Benchmark::~Benchmark() {}

std::string Benchmark::HandleMessage(const std::string &msg) {
	std::string empty_str;
	return empty_str;
}

void Benchmark::reportMessage(const std::string &msg) {
    if (msg.empty())
        return;
    setting.sendMessageToSetting(msg);
}

void Benchmark::updatePlan(const PlanConfig &pc, std::vector<OP_TYPE>& plan) {
    uint32_t total_size = pc.update_percent + pc.read_percent + pc.insert_percent;
    plan.resize(total_size);
    fill_n(plan.begin(),pc.read_percent,OP_TYPE::SEARCH);
    fill_n(plan.begin() + pc.read_percent,pc.insert_percent,OP_TYPE::INSERT);
    fill_n(plan.begin() + pc.read_percent + pc.insert_percent,pc.update_percent,OP_TYPE::UPDATE);
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
