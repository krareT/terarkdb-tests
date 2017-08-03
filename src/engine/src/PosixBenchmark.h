//
// Created by terark on 9/10/16.
//

#ifndef TERARKDB_TEST_FRAMEWORK_POSIXBENCHMARK_H
#define TERARKDB_TEST_FRAMEWORK_POSIXBENCHMARK_H

#include <src/Benchmark.h>
#include <src/Setting.h>
class PosixBenchmark : public Benchmark{
private:
    virtual void Open(void) override ;
    virtual void Close(void) override;
    virtual void Load(void) override ;
    virtual bool ReadOneKey(ThreadState*) override;
    virtual bool UpdateOneKey(ThreadState *) override;
    virtual bool InsertOneKey(ThreadState *)  override ;
    virtual bool VerifyOneKey(ThreadState *) override;
public:
    PosixBenchmark(Setting& setting);
    virtual ThreadState* newThreadState(const std::atomic<std::vector<bool>*>* whichSamplingPlan) override;
    virtual bool Compact(void) override;

};


#endif //TERARKDB_TEST_FRAMEWORK_POSIXBENCHMARK_H
