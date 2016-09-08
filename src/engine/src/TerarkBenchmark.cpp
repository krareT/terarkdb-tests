//
// Created by terark on 8/24/16.
//

#include "TerarkBenchmark.h"

void TerarkBenchmark::PrintHeader() {
    fprintf(stdout, "NarkDB Test Begins!");
}

void TerarkBenchmark::Close() {
    tab->safeStopAndWaitForFlush();
    tab = NULL;
}

void TerarkBenchmark::Load(void) {
    DoWrite(true);
    //tab->compact();
}

std::string TerarkBenchmark::getKey(std::string &str) {
    std::vector<std::string> strvec;
    boost::split(strvec, str, boost::is_any_of("\t"));
    return strvec[2] + '\0' + strvec[7];
}

void TerarkBenchmark::Open() {
    PrintHeader();
    assert(tab == NULL);
    std::cout << "Open database " << setting.FLAGS_db << std::endl;
    tab = CompositeTable::open(setting.FLAGS_db);
    assert(tab != NULL);
    indexId = tab->getIndexId("cur_title,cur_timestamp");
    assert(indexId < tab->getIndexNum());
    tab->setThrowOnThrottle(true);
}

void TerarkBenchmark::DoWrite(bool seq) {
    std::cout << "Write the data to terark : " << setting.getLoadDataPath() << std::endl;
    std::string str;
    long long recordnumber = 0;
    const Schema &rowSchema = tab->rowSchema();
    valvec<byte_t> row;
    DbContextPtr ctx = tab->createDbContext();
    ctx->syncIndex = setting.FLAGS_sync_index;
    FILE *loadFile = fopen(setting.getLoadDataPath().c_str(), "r");
    assert(loadFile != NULL);
    posix_fadvise(fileno(loadFile), 0, 0, POSIX_FADV_SEQUENTIAL);
    char *buf;
    size_t n = 0;
    buf = NULL;
    std::string key;
    while (getline(&buf, &n, loadFile) != -1) {
        str = buf;
        if (rowSchema.columnNum() != rowSchema.parseDelimText('\t', str, &row)) {
            std::cerr << "ERROR STR:" << str << std::endl;
            continue;
        }
        llong rid = ctx->upsertRow(row);
        if (rid < 0) { // unique index
            printf("Insert failed: %s\n", ctx->errMsg.c_str());
        }
        assert(VerifyOneKey(rid, row, ctx) == true);
        key = getKey(str);
        pushKey(key);
        recordnumber++;
        if (recordnumber % 100000 == 0)
            std::cout << "Insert reocord number: " << recordnumber / 10000 << "w" << std::endl;
        if (n > 1024 * 1024) {
            free(buf);
            buf = NULL;
            n = 0;
        }
    }
    if (buf != NULL) {
        free(buf);
    }
    time_t now;
    struct tm *timenow;
    time(&now);
    timenow = localtime(&now);
    printf("recordnumber %lld, time %s\n", recordnumber, asctime(timenow));
}

bool TerarkBenchmark::VerifyOneKey(llong rid, valvec<byte> &outside, DbContextPtr &ctx) {
    valvec<byte> inside;
    ctx->getValue(rid, &inside);
    assert(inside.size() == outside.size());
    for (int i = 0; i < inside.size(); i++) {
        if (inside[i] != outside[i])
            return false;
    }
    return true;
}

bool TerarkBenchmark::ReadOneKey(ThreadState *thread) {
    std::string &rkey = thread->key;

    if (false == getRandomKey(rkey, thread->randGenerator)) {
        std::cout << "allkeys empty" << std::endl;
        return false;
    }
    fstring key(rkey);
    tab->indexSearchExact(indexId, key, &(thread->idvec), thread->ctx.get());
    //assert(idvec.size() <= 1);
    if (thread->idvec.size() == 0)
        return false;
    thread->ctx->getValue(thread->idvec[0], &(thread->row));
    return true;
}

bool TerarkBenchmark::UpdateOneKey(ThreadState *thread) {
    std::string &rkey = thread->key;

    if (false == getRandomKey(rkey, thread->randGenerator)) {
        std::cout << "allkeys empty" << std::endl;
        return false;
    }

    fstring key(rkey);
    tab->indexSearchExact(indexId, key, &(thread->idvec), thread->ctx.get());
    //assert(idvec.size() <= 1);
    if (thread->idvec.size() == 0) {
        return false;
    }
    thread->ctx->getValue(thread->idvec[0], &(thread->row));
    try {
        llong rid = thread->ctx->upsertRow(thread->row);
        if (rid < 0) { // unique index
            printf("Insert failed: %s\n", thread->ctx->errMsg.c_str());
            return false;
        }
    } catch (const std::exception &e) {
        return false;
    }
    return true;
}

bool TerarkBenchmark::InsertOneKey(ThreadState *thread) {
    static const Schema &rowSchema = tab->rowSchema();
    std::string &rstr = thread->str;

    if (updateDataCq.try_pop(rstr) == false) {
        return false;
    }
    if (rowSchema.columnNum() != rowSchema.parseDelimText('\t', rstr, &(thread->row))) {
        std::cerr << "InsertOneKey error:" << rstr << std::endl;
        return false;
    }

    try {
        if (thread->ctx->upsertRow(thread->row) < 0) { // unique index

            printf("Insert failed: %s\n", thread->ctx->errMsg.c_str());
            return false;
        }
    } catch (const NeedRetryException &e) {
        updateDataCq.push(rstr);
        return false;
    } catch (const WriteThrottleException &e) {
        updateDataCq.push(rstr);
        return false;
    } catch (const std::exception &e) {
        return false;
    }
    thread->key = getKey(rstr);
    pushKey(thread->key);
    return true;
}

bool TerarkBenchmark::Compact() {
    tab->compact();
}

ThreadState *
TerarkBenchmark::newThreadState(std::atomic<std::vector<uint8_t> *> *whichEPlan,
                                std::atomic<std::vector<uint8_t> *> *whichSPlan) {
    return new ThreadState(threads.size(), whichEPlan, whichSPlan, &tab);
}

