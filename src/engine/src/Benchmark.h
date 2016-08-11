//
// Created by terark on 16-8-11.
//

#ifndef TERARKDB_TEST_FRAMEWORK_BENCHMARK_H
#define TERARKDB_TEST_FRAMEWORK_BENCHMARK_H
#include"src/leveldb.h"
#include "Setting.h"

class Benchmark{
private:
public:
    std::vector<std::pair<std::thread,ThreadState*>> threads;
    Setting &setting;
    Benchmark(Setting &s):setting(s){};
    virtual void  Run(void) final {
        Open();
        Load();
        RunBenchmark();
        Close();
    };
    virtual void Open(void) = 0;
    virtual void Load(void) = 0;
    virtual void Close(void) = 0;
    virtual bool ReadOneKey(ThreadState*) = 0;
    virtual bool WriteOneKey(ThreadState*) = 0;
    virtual ThreadState* newThreadState(std::atomic<std::vector<uint8_t >*>* which) = 0;
    static void ThreadBody(Benchmark *bm,ThreadState* state){
        bm->ReadWhileWriting(state);
    }
    void updatePlan(std::vector<uint8_t> &plan, uint8_t readPercent){
        assert(readPercent <= 100);
        if (plan.size() != 100){
            plan.resize(100);
        }
        std::fill_n(plan.begin(),readPercent,1);
        std::fill(plan.begin()+readPercent,plan.end(),0);
        std::shuffle(plan.begin(),plan.end(),std::default_random_engine());
    }
    void adjustThreadNum(uint32_t target, std::atomic<std::vector<uint8_t >*>* which){

        while (target > threads.size()){
            //Add new thread.
            ThreadState* state = newThreadState(which);
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
    void RunBenchmark(void){
        int old_readPercent = -1;
        std::atomic<std::vector<uint8_t > *> planAddr;
        std::vector<uint8_t > plan[2];
        bool backupPlan = false;//不作真假，只用来切换plan
        while( !setting.baseSetting.ifStop()){

            int readPercent = setting.baseSetting.getReadPercent();
            if (readPercent != old_readPercent){
                old_readPercent = readPercent;
                updatePlan(plan[backupPlan],readPercent);
                planAddr.store( &(plan[backupPlan]));
                backupPlan = !backupPlan;
            }
            int threadNum = setting.baseSetting.getThreadNums();
            adjustThreadNum(threadNum,&planAddr);
            sleep(5);
        }
        adjustThreadNum(0, nullptr);
    }
    void ReadWhileWriting(ThreadState *thread) {

        std::cout << "Thread " << thread->tid << " start!" << std::endl;
        std::unordered_map<uint8_t, bool (Benchmark::*)(ThreadState *thread)> func_map;
        func_map[1] = &Benchmark::ReadOneKey;
        func_map[0] = &Benchmark::WriteOneKey;

        struct timespec start,end;
        while (thread->STOP.load() == false) {

            std::vector<uint8_t > *plan = (*(thread->which)).load();
            for (auto each : *plan) {
                clock_gettime(CLOCK_MONOTONIC,&start);
                if ((this->*func_map[each])(thread)) {
                    clock_gettime(CLOCK_MONOTONIC,&end);
                    thread->stats->FinishedSingleOp(each,&start,&end);
                }
            }
        }
        std::cout << "Thread " << thread->tid << " stop!" << std::endl;
    }
    std::string GatherTimeData(){
        std::stringstream ret;
        for( auto& eachThread : threads){
            ret << "Thread " << Stats::readTimeDataCq.unsafe_size() << std::endl;
        }
        return ret.str();
    }
};

#endif //TERARKDB_TEST_FRAMEWORK_BENCHMARK_H
