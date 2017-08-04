//
// Created by terark on 16-8-12.
//
#include "Benchmark.h"
#include <fstream>
#include <terark/util/autoclose.hpp>
#include <terark/util/linebuf.hpp>
#include <terark/util/profiling.hpp>

using namespace terark;

/**
 * 更新采样的plan
 * 将plan随机设置percent个true
 * @param plan
 * @param percent
 */
void Benchmark::updateSamplingPlan(std::vector<bool> &plan, uint8_t percent) {
    if (percent > 100)
        return ;
    plan.resize(100);
    std::fill_n(plan.begin(),percent, true);
    std::shuffle(plan.begin(),plan.end(),std::default_random_engine());
}

/**
 * 随机设置plan
 * @param plan
 */

void Benchmark::shufflePlan(std::vector<uint8_t > &plan){
    std::shuffle(plan.begin(),plan.end(),std::default_random_engine());
}

/**
 * 调整线程个数
 * 如果目标线程个数大于当前线程个数，则创建线程
 * 如果目标线程小于当前线程个数，则从线程数组中从最后取出相应数量的线程销毁
 * @param target
 * @param whichSPlan
 */
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

/**
 * 调整采样plan
 * 更新samplingRate来调整采样plan
 * 每次调整完成之后更新samplingPlanAddr
 * 并将whichSPlan设置为!whichSPlan
 * @param samplingRate
 */
void Benchmark::adjustSamplingPlan(uint8_t samplingRate){
    updateSamplingPlan(samplingPlan[whichSPlan], samplingRate);
    samplingPlanAddr.store(& (samplingPlan[whichSPlan]));
    whichSPlan = !whichSPlan;
}

/**
 * 运行Benchmark
 * 执行压缩和插入操作
 */
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
}

/**
 * 执行一次操作，通过samplingPlan的最后一个记录来判断是否需要采样
 * 如果需要采样并且是查找操作或执行与OP_TYPE相对应的派生类函数返回True就记录执行的开始时间和结束时间
 * 否则只单纯执行操作
 * @param state
 * @param type
 * @return
 */
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

/**
 * 在进行写操作的时候进行读操作
 * 在线程未被终止前每次执行一个操作
 * @param thread
 */
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

/**
 * 从文件中加载key
 */
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

/**
 * 获取随机的key
 * @param key
 * @param rg
 * @return
 */
bool Benchmark::getRandomKey(std::string &key,std::mt19937_64 &rg) {
    if (allkeys.empty()){
        return false;
    }
    auto randomIndex = rg() % allkeys.size();
    const char*  p = allkeys.beg_of(randomIndex);
    const size_t n = allkeys.slen(randomIndex);
    key.assign(p, n);
    return true;
}

/**
 * 加载插入数据
 */
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

/**
 * 验证
 * 创建加载验证KvData线程，并且保证这个线程未执行完之前不会继续执行父线程
 * 在程序的Stop未true之前，更新SamplingRate
 * 再更新验证线程数量
 * 检查执行的Plan是否正确
 * 在程序的Stop变成true之后，将验证线程全部销毁
 */
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

/**
 * 调整验证线程数目
 * 目标多则创建目标少则销毁
 * @param target
 * @param whichPlan
 */
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

/**
 * 执行验证
 * 在程序未Stop之前，通过samplingPlan的最后一个记录来判断是否需要采样
 * 如果需要采样，则记录时间
 * 如果不需要采样则单纯记录时间
 * 如果执行VerifyOneKey成功，则判断验证结果
 * 如果是MISMATCH或FAIL则记录到verifyFailDataCq
 * @param thread
 */
void Benchmark::executeVerify(ThreadState *thread) {
    fprintf(stderr, "Thread %2d run Benchmark::verifyOneKey() start...\n", thread->tid);
    char *tempStr= new char[748578000 * 2];
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

/**
 * 加载验证KV数据
 * 首先通过setting读取验证kv文件
 * 然后计算读取的开始时间和结束时间
 * 最后输出信息
 */
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
            line.chomp();
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
    if (!setting.ifStop() && lines > 0) {
        fprintf(stderr, "Benchmark::loadVerifykvData(): all data are loaded");
    }
}

/**
 * 清除线程
 */
void Benchmark::clearThreads() {
  for (auto& e : this->threads) {
    delete e.second;
    e.second = NULL;
  }
  this->threads.clear();
}

/**
 * Run操作，一般由派生类执行
 * 首先执行派生类的Open函数
 * 判断Setting厘米操作是哪种操作
 * 如果是load，那么就先把key全部清除，再执行派生类的Load()函数加载key
 * 如果是compact,就执行派生类的Compact()函数
 * 如果是verify,就执行验证
 * 如果都不是，则首先执行Benchmark的loadKeys()函数再执行RunBenchmark()函数
 * 最后执行派生类的Close()函数
 */
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
        loadKeys();
        RunBenchmark();
    }
    Close();
}

/**
 * 构造函数
 * 首先将executeFuncMap赋值为ReadOneKey,InsertOneKey,UpdateOneKey的地址
 * 再将setting初始化
 * 最后初始化压缩次数
 * @param s
 */
Benchmark::Benchmark(Setting& s)
: executeFuncMap{&Benchmark::ReadOneKey,
                 &Benchmark::InsertOneKey,
                 &Benchmark::UpdateOneKey,
        }
, setting(s)
{
    compactTimes = s.getCompactTimes();
}

/**
 * 析构函数
 */
Benchmark::~Benchmark() {}

/**
 * 返回一个未初始化empty_str
 * @param msg
 * @return
 */
std::string Benchmark::HandleMessage(const std::string &msg) {
	std::string empty_str;
	return empty_str;
}

/**
 * 在setting中更新response_message_cq;
 * 若msg非空，则添加
 * @param msg
 */
void Benchmark::reportMessage(const std::string &msg) {
    if (!msg.empty())
      setting.sendMessageToSetting(msg);
}

/**
 * 更新plan
 * @param pc
 * @param plan
 */
void Benchmark::updatePlan(const PlanConfig &pc, std::vector<OP_TYPE>& plan) {
    uint32_t total_size = pc.update_percent + pc.read_percent + pc.insert_percent;
    plan.resize(total_size);
    fill_n(plan.begin(),pc.read_percent,OP_TYPE::SEARCH);
    fill_n(plan.begin() + pc.read_percent,pc.insert_percent,OP_TYPE::INSERT);
    fill_n(plan.begin() + pc.read_percent + pc.insert_percent,pc.update_percent,OP_TYPE::UPDATE);
    std::shuffle(plan.begin(),plan.end(),std::default_random_engine());
}

/**
 * 检查执行的Plan
 * 对于每一个线程，首先获取其PlanConfig，再与与其相对应的ThreadState中的planConfig进行比对
 * 若相同则检查下一线程
 * 若不同则更新当前线程的planConfig
 */
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
