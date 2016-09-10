//
// Created by terark on 8/24/16.
//

#include "TerarkBenchmark.h"

void TerarkBenchmark::PrintHeader() {
    fprintf(stdout, "NarkDB Test Begins!");
}

void TerarkBenchmark::Close() {
    tab->safeStopAndWaitForCompress();
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
    colgroupId = tab->getColgroupId("str34");
    assert(colgroupId < tab->getColumnNum());
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
            printf("Update failed: %s\n", thread->ctx->errMsg.c_str());
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

std::string TerarkBenchmark::HandleMessage(const std::string &msg) {
    std::stringstream ss;
    if (msg.empty())
        return ss.str();
    static std::unordered_map<std::string, std::pair<bool (TerarkBenchmark::*)(const std::string &),
            std::string (TerarkBenchmark::*)(void)>> handleFuncMap;
    handleFuncMap["write_throttle"] = std::make_pair(&TerarkBenchmark::updateWriteThrottle,
                                                     &TerarkBenchmark::getWriteThrottle);
    handleFuncMap["colgroup_mmapPopulate"] = std::make_pair(&TerarkBenchmark::updateColGroupMmapPopulate,
                                                            &TerarkBenchmark::getColGroupMmapPopulate);
    handleFuncMap["index_mmappopulate"] = std::make_pair(&TerarkBenchmark::updateIndexMmapPopulate,
                                                         &TerarkBenchmark::getIndexMmapPopulate);
    handleFuncMap["checksumLevel"] = std::make_pair(&TerarkBenchmark::updateCheckSumLevel,
                                                    &TerarkBenchmark::getCheckSumLevel);
    handleFuncMap["dictziosampleratio"] = std::make_pair(&TerarkBenchmark::updateDictZipSampleRatio,
                                                         &TerarkBenchmark::getDictZipSampleRatio);
    handleFuncMap["minmergesegnum"] = std::make_pair(&TerarkBenchmark::updateMinMergeSetNum,
                                                     &TerarkBenchmark::getMinMergeSetNum);
    handleFuncMap["purgedeletethreshold"] = std::make_pair(&TerarkBenchmark::updatePurgeDeleteThreshold,
                                                           &TerarkBenchmark::getPurgeDeleteThreshold);
    handleFuncMap["maxwritingsegmentsize"] = std::make_pair(&TerarkBenchmark::updateMaxWritingSegmentSize,
                                                            &TerarkBenchmark::getMaxWritingSegmentSize);
    size_t div = msg.find(':');
    std::string key = msg.substr(0, div);
    std::string value = msg.substr(div + 1);
    (this->*handleFuncMap[key].first)(value);


    for (auto each : handleFuncMap) {
        ss << each.first << ":" << (this->*each.second.second)() << std::endl;
    }
}

bool TerarkBenchmark::updateWriteThrottle(const std::string &val) {

    size_t bytes;
    for (auto ch : val) {

        if (isdigit(ch)) {
            bytes = bytes * 10 + ch & 0x0f;
        } else if (ch == 'K' || ch == 'k') {
            bytes = bytes * 1024;
            break;
        } else if (ch == 'M' || ch == 'm') {
            bytes = bytes * 1024 * 1024;
            break;
        }
    }
    tab->getSchemaConfig().m_writeThrottleBytesPerSecond = bytes;

    return true;
}

bool TerarkBenchmark::updateColGroupMmapPopulate(const std::string &val) {
    if (val == "true")
        tab->getColgroupSchemaForChange(colgroupId).m_mmapPopulate = true;
    else if (val == "false")
        tab->getColgroupSchemaForChange(colgroupId).m_mmapPopulate = true;
    else
        return false;
    return true;
}

bool TerarkBenchmark::updateCheckSumLevel(const std::string &val) {
    int level = stoi(val);
    tab->getColgroupSchemaForChange(colgroupId).m_checksumLevel = level;
    return true;
}

bool TerarkBenchmark::updateDictZipSampleRatio(const std::string &val) {
    float value = sscanf(val.c_str(), "%f", &value);
    tab->getColgroupSchemaForChange(colgroupId).m_dictZipSampleRatio = value;
    return true;
}

bool TerarkBenchmark::updateIndexMmapPopulate(const std::string &val) {
    if (val == "true")
        tab->getIndexSchemaForChange(indexId).m_mmapPopulate = true;
    else if (val == "false")
        tab->getIndexSchemaForChange(indexId).m_mmapPopulate = false;
    else
        return false;
    return true;
}

std::string TerarkBenchmark::getWriteThrottle(void) {

    std::stringstream ss;
    ss << tab->getSchemaConfig().m_writeThrottleBytesPerSecond;
    return ss.str();
}

std::string TerarkBenchmark::getColGroupMmapPopulate(void) {

    std::stringstream ss;
    ss << tab->getColgroupSchemaForChange(colgroupId).m_mmapPopulate;
    return ss.str();
}

std::string TerarkBenchmark::getIndexMmapPopulate(void) {

    std::stringstream ss;
    ss << tab->getIndexSchemaForChange(colgroupId).m_mmapPopulate;
    return ss.str();
}

std::string TerarkBenchmark::getCheckSumLevel(void) {
    std::stringstream ss;
    ss << tab->getColgroupSchemaForChange(colgroupId).m_checksumLevel;
    return ss.str();
}

std::string TerarkBenchmark::getDictZipSampleRatio(void) {
    std::stringstream ss;
    ss << tab->getColgroupSchemaForChange(colgroupId).m_dictZipSampleRatio;
    return ss.str();

}

bool TerarkBenchmark::updateMinMergeSetNum(const std::string &val) {
    size_t value = stoi(val);
    tab->getSchemaConfig().m_minMergeSegNum = value;
    return true;
}

std::string TerarkBenchmark::getMinMergeSetNum(void) {
    std::stringstream ss;
    ss << tab->getSchemaConfig().m_minMergeSegNum;
    return ss.str();

}

bool TerarkBenchmark::updatePurgeDeleteThreshold(const std::string &val) {

    double value;
    if (sscanf(val.c_str(), "%e", &value) != 1)
        return false;
    tab->getSchemaConfig().m_purgeDeleteThreshold = value;
    return true;
}

std::string TerarkBenchmark::getPurgeDeleteThreshold(void) {

    std::stringstream ss;
    ss << tab->getSchemaConfig().m_purgeDeleteThreshold;
    return ss.str();
}

bool TerarkBenchmark::updateMaxWritingSegmentSize(const std::string &val) {

    llong value = stoll(val);
    tab->getSchemaConfig().m_maxWritingSegmentSize = value;
    return true;
}

std::string TerarkBenchmark::getMaxWritingSegmentSize(void) {
    std::stringstream ss;
    ss << tab->getSchemaConfig().m_maxWritingSegmentSize;
    return ss.str();
}

