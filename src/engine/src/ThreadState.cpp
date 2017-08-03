//
// Created by terark on 8/24/16.
//

#include "ThreadState.h"
#include <assert.h>

/**
 * 构造函数
 * 初始化几个参数：
 *      tid(线程id)
 *      whichSamplingPlan(选用哪个采样plan)
 *      STOP(线程是否停止)
 *      seed(随机数生成种子)
 *      randGenerator(mt19937_64类的一个对象)
 * @param index
 * @param wsp
 */
ThreadState::ThreadState(int index, const std::atomic<std::vector<bool>*>* wsp)
  : tid(index), whichSamplingPlan(wsp) {
    STOP.store(false);
    seed = tid;
    randGenerator.seed(seed);
}

/**
 * 析构函数
 * 空函数，啥也没有
 */
ThreadState::~ThreadState(){
}
