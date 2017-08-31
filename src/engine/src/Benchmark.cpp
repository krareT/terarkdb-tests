//
// Created by terark on 16-8-12.
//
#include "Benchmark.h"
#include <fstream>
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
void Benchmark::adjustThreadNum(uint32_t target, const std::atomic<std::vector<bool>*>* whichSPlan) {
    while (target > threads.size()){
        ThreadState* state = newThreadState(whichSPlan);
        state->planConfig.read_percent = 100;
        state->planConfig.insert_percent = 0;
        state->planConfig.update_percent = 0;
        updatePlan(state->planConfig,state->executePlan[0]);
        state->whichPlan.store(0,std::memory_order_relaxed);
        threads.emplace_back(std::thread([=](){ReadWhileWriting(state);}), state);
    }
    while (target < threads.size()){
        threads.back().second->STOP.store(true);
        threads.back().first.join();
        delete threads.back().second;
        threads.pop_back();
    }
}

void Benchmark::adjustSamplingPlan(uint8_t samplingRate){
    updateSamplingPlan(samplingPlan[whichSPlan], samplingRate);
    samplingPlanAddr.store(& (samplingPlan[whichSPlan]));
    whichSPlan = !whichSPlan;
}

void Benchmark::RunBenchmark(void){
    int old_samplingRate = -1;
    std::thread loadInsertDataThread(&Benchmark::loadInsertData, this);
#if 0
    std::unique_ptr<std::thread> loadShufKeyDataThreadPtr;
    if (setting.useShufKey) {
        loadShufKeyDataThreadPtr.reset(new std::thread(&Benchmark::loadShufKeyData, this));
    }
#endif

  loadShufKeyData();

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
            std::thread compactThread([this](){
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
#if 0
    if (loadShufKeyDataThreadPtr) {
        loadShufKeyDataThreadPtr->join();
    }
#endif
}

bool Benchmark::executeOneOperation(ThreadState* state, OP_TYPE type){
    assert(executeFuncMap[int(type)] != 0);
    std::vector<bool>& samplingPlan = *state->whichSamplingPlan->load();
    bool useSampling;
    if (state->samplingRecord[int(type)] < samplingPlan.size()){
        useSampling = samplingPlan[state->samplingRecord[int(type)]++];
    } else {
        useSampling = samplingPlan[0];
        state->samplingRecord[int(type)] = 1;
    }
    if (useSampling) {
        struct timespec start,end;
        clock_gettime(CLOCK_REALTIME, &start);
        bool ret = (this->*executeFuncMap[int(type)])(state);
        if (ret || type == OP_TYPE::SEARCH) {
            clock_gettime(CLOCK_REALTIME, &end);
            Stats::FinishedSingleOp(type, start, end);
        }
        return ret;
    } else {
        return ((this->*executeFuncMap[int(type)])(state));
    }
}

void Benchmark::ReadWhileWriting(ThreadState *thread) {
    fprintf(stderr, "Thread %2d run Benchmark::ReadWhileWriting() start...\n", thread->tid);
    while (!thread->STOP.load()) {
        const auto &executePlan = thread->executePlan[thread->whichPlan.load(std::memory_order_relaxed)];
        for (auto type : executePlan) {
            executeOneOperation(thread,type);
        }
    }
    fprintf(stderr, "Thread %2d run Benchmark::ReadWhileWriting() exit...\n", thread->tid);
}

void Benchmark::loadKeys() {
    fprintf(stderr, "Benchmark::loadKeys(): %s\n", setting.getKeysDataPath().c_str());
    std::ifstream keysFile(setting.getKeysDataPath());
    assert(keysFile.is_open());
    std::string str;
    while (std::getline(keysFile, str)) {
        if (str.size())
            allkeys.push_back(str);
    }
    allkeys.shrink_to_fit();
    keysFile.close();
    fprintf(stderr, "Benchmark::loadKeys(): allkeys.size() = %zd\n", allkeys.size());
}

bool Benchmark::getRandomKey(std::string &key,std::mt19937_64 &rg) {
    if (!setting.useShufKey) {
        if (allkeys.empty()){
            return false;
        }
        auto randomIndex = rg() % allkeys.size();
        const char*  p = allkeys.beg_of(randomIndex);
        const size_t n = allkeys.slen(randomIndex);
        key.assign(p, n);
        return true;
    } else {
#if 0
        return shufKeyDataCq.try_pop(key);
#endif
      LineBuf line;
        if(!feof(*ifs)) {
          if (line.getline((*ifs)) > 0) {
            line.chomp();
            key.assign(line.p, line.n);
          } else {
            return false;
          }
        } else {
          extern void stop_test();
          stop_test();
          return false;
        }
      //fprintf(stderr, "key: %s\n", key.c_str());
      return true;
    }
}

void Benchmark::loadInsertData(){
    const char* fpath = setting.getInsertDataPath().c_str();
    Auto_fclose ifs(fopen(fpath, "r"));
    if (!ifs) {
        fprintf(stderr, "ERROR: Benchmark::loadInsertData(): fopen(%s, r) = %s\n", fpath, strerror(errno));
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
            //line.chomp();
            bytes += line.size();
            updateDataCq.push(std::string(line.p, line.n));
            count++;
        }
        lines += count;
    //  printf("Benchmark::loadInsertData(): total = %9d   chunklines = %6d\n", lines, count);
        usleep(300000);
    }
    long long t1 = pf.now();
    fprintf(stderr
        , "Benchmark::loadInsertData(%s) %s, lines = %zd, bytes = %zd, time = %f sec, speed = %f MB/sec\n"
        , fpath
        , setting.ifStop() ? "stopped" : "completed"
        , lines, bytes, pf.sf(t0,t1), bytes/pf.uf(t0,t1)
        );
    if (!setting.ifStop() && lines > 0) {
        fprintf(stderr, "Benchmark::loadInsertData(): all data are loaded, wait for %f sec then compact!\n", pf.sf(t0,t1)/2);
        usleep(pf.us(t0,t1)/2);
        fprintf(stderr, "Benchmark::loadInsertData(): start compact ...\n");
        Compact();
        fprintf(stderr, "Benchmark::loadInsertData(): compaction finished!\n");
    }
}


void Benchmark::Verify() {
    int old_samplingRate = -1;
    std::thread loadVerifyDataThread(&Benchmark::loadVerifyKvData, this);
    while (!setting.ifStop()) {
        int samplingRate = setting.getSamplingRate();
        if (old_samplingRate != samplingRate) {
            old_samplingRate = samplingRate;
            adjustSamplingPlan(samplingRate);
        }
        int threadNum = setting.getThreadNums();
        adjustVerifyThreadNum(threadNum, &samplingPlanAddr);
        checkExecutePlan();
        int handle_messge_times = 5;
        while (handle_messge_times--) {
            reportMessage(HandleMessage(setting.getMessage()));
            sleep(5);
        }
    }
    adjustVerifyThreadNum(0, nullptr);
    loadVerifyDataThread.join();
}

void Benchmark::adjustVerifyThreadNum(uint32_t target, const std::atomic<std::vector<bool>*>* whichPlan) {
    while (target > threads.size()) {
        ThreadState *state = newThreadState(whichPlan);
        threads.emplace_back(std::thread([=](){executeVerify(state);}), state);
    }
    while (target < threads.size()){
        threads.back().second->STOP.store(true);
        threads.back().first.join();
        delete threads.back().second;
        threads.pop_back();
    }
}

void Benchmark::executeVerify(ThreadState *thread) {
    fprintf(stderr, "Thread %2d run Benchmark::verifyOneKey() start...\n", thread->tid);
    char *tempStr= new char[3598300 * 2];
    while (!thread->STOP.load()) {
      std::vector<bool> &samplingPlan = *thread->whichSamplingPlan->load();
      bool useSampling;
      if (thread->verifySamplingRecord < samplingPlan.size()) {
        useSampling = samplingPlan[thread->verifySamplingRecord++];
      } else {
        useSampling = samplingPlan[0];
        thread->verifySamplingRecord = 1;
      }
      bool ret;
      if (useSampling) {
        struct timespec start, end;
        clock_gettime(CLOCK_REALTIME, &start);
        ret = VerifyOneKey(thread);
        if (ret) {
          clock_gettime(CLOCK_REALTIME, &end);
          Stats::FinishedSingleOp(OP_TYPE(int(thread->verifyResult)), start, end);
        }
      } else {
        ret = VerifyOneKey(thread);
      }
      if(ret) {
        if(thread->verifyResult == VERIFY_TYPE::MISMATCH) {
          sprintf(tempStr, "MISMATCH: key(%s)\tstoreValue(%s)\tvalue(%s)\n"
                  , thread->key.c_str()
                  , thread->storeValue.c_str()
                  , thread->value.c_str());
          Stats::RecordVerifyData(tempStr);
          fprintf(stderr, "MISMATCH: key(%s)\n", thread->key.c_str());
        } else if (thread->verifyResult == VERIFY_TYPE::FAIL) {
          sprintf(tempStr, "FAIL: key(%s)\n", thread->key.c_str());
          Stats::RecordVerifyData(tempStr);
          fprintf(stderr, "%s", tempStr);
        }
      }
    }
    delete tempStr;
    fprintf(stderr, "Thread %2d run Benchmark::verifyOneKey() exit...\n", thread->tid);
}

void Benchmark::loadVerifyKvData() {
    const char* fpath = setting.getVerifyKvFile().c_str();
    Auto_fclose ifs(fopen(fpath, "r"));
    if (!ifs) {
        fprintf(stderr, "ERROR: Benchmark::loadVerifyKvData(): fopen(%s, r) = %s\n", fpath, strerror(errno));
        return;
    }
    size_t limit = setting.FLAGS_load_size;
    fprintf(stderr, "Benchmark::loadVerifyKvData(%s) start, limit = %f GB\n", fpath, limit/1e9);
    LineBuf line;
    size_t lines = 0;
    terark::profiling pf;
    long long t0 = pf.now();
    size_t bytes = 0;
    while(bytes < limit && !setting.ifStop() && !feof(ifs)) {
        size_t count = 0;
        while (bytes < limit && verifyDataCq.unsafe_size() < 200000 && line.getline(ifs) > 0) {
            //line.chomp();
            bytes += line.size();
            verifyDataCq.push(std::string(line.p, line.n));
            count++;
        }
        lines += count;
        usleep(300000);
    }
    long long t1 = pf.now();
    fprintf(stderr
    , "Benchmark::loadVerifyKvData(%s) %s, lines = %zd, bytes = %zd, time = %f sec, speed = %f MB/sec\n"
    , fpath
    , setting.ifStop() ? "stopped" : "completed"
    , lines, bytes, pf.sf(t0, t1), bytes/pf.uf(t0, t1)
    );

    // when read all verify_kv_file, stop process
    if (feof(ifs)) {
      extern void stop_test();
      stop_test();
    }
    if (!setting.ifStop() && lines > 0) {
        fprintf(stderr, "Benchmark::loadVerifykvData(): all data are loaded");
    }
}

void Benchmark::loadShufKeyData() {
#if 0
    const char* fpath = setting.getKeysDataPath().c_str();
    Auto_fclose ifs(fopen(fpath, "r"));
    if (!ifs) {
        fprintf(stderr, "ERROR: Benchmark::loadShufKeyData(): fopen(%s, r) = %s\n", fpath, strerror(errno));
        return;
    }
    size_t limit = setting.FLAGS_load_size;
    size_t lines;
    while(!setting.ifStop()) {
        fprintf(stderr, "Benchmark::loadShufKeyData(%s) start, limit = %f GB\n", fpath, limit/1e9);
        LineBuf line;
        lines = 0;
        terark::profiling pf;
        long long t0 = pf.now();
        size_t bytes = 0;
        while(bytes < limit && !setting.ifStop() && !feof(ifs)) {
            size_t count = 0;
            while (bytes < limit && shufKeyDataCq.unsafe_size() < 200000 && line.getline(ifs) > 0) {
                line.chomp();
                bytes += line.size();
                shufKeyDataCq.push(std::string(line.p, line.n));
                count++;
            }
            lines += count;
            usleep(300000);
        }
        long long t1 = pf.now();
        fprintf(stderr
                , "Benchmark::loadShufKeyData(%s) %s, lines = %zd, bytes = %zd, time = %f sec, speed = %f MB/sec\n"
                , fpath
                , setting.ifStop() ? "stopped" : "completed"
                , lines, bytes, pf.sf(t0, t1), bytes/pf.uf(t0, t1)
        );

        // when read all key data, begin again
        if (feof(ifs)) {
            rewind(ifs);
        }
    }
    if (!setting.ifStop() && lines > 0) {
        fprintf(stderr, "Benchmark::loadShufKeyData(): all data are loaded");
    }
#endif

  const char* fpath = setting.getKeysDataPath().c_str();
  ifs.reset(new Auto_fclose(fopen(fpath, "r")));
  if (!ifs) {
    fprintf(stderr, "ERROR: Benchmark::loadShufKeyData(): fopen(%s, r) = %s\n", fpath, strerror(errno));
    return;
  }
}

void Benchmark::clearThreads() {
  for (auto& e : this->threads) {
    delete e.second;
    e.second = NULL;
  }
  this->threads.clear();
}

void Benchmark::Run(void) {
    Open();
    if (setting.getAction() == "load") {
        try {
            allkeys.erase_all();
            Load();
        }
        catch (const std::exception& e) {
            fprintf(stderr, "ERROR: %s: %s", BOOST_CURRENT_FUNCTION, e.what());
        }
    }
    else if (setting.getAction() == "compact") {
        this->Compact();
    }
    else if (setting.getAction() == "verify") {
        this->Verify();
    }
    else {
        if (!setting.useShufKey) {
            loadKeys();
        }
        RunBenchmark();
    }
    Close();
}

Benchmark::Benchmark(Setting& s)
: executeFuncMap{&Benchmark::ReadOneKey,
                 &Benchmark::InsertOneKey,
                 &Benchmark::UpdateOneKey,
        }
, setting(s)
{
    compactTimes = s.getCompactTimes();
    shufKeyIndex = 0;
}

Benchmark::~Benchmark() {}

std::string Benchmark::HandleMessage(const std::string &msg) {
	std::string empty_str;
	return empty_str;
}

void Benchmark::reportMessage(const std::string &msg) {
    if (!msg.empty())
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
