//
// Created by terark on 8/24/16.
//

#include "TerarkBenchmark.h"
#include <terark/util/autoclose.hpp>
#include <terark/util/linebuf.hpp>

TerarkBenchmark::TerarkBenchmark(const Setting &setting1)
: Benchmark(setting1) {
  indexId = size_t(-1);
  colgroupId = size_t(-1);
}
TerarkBenchmark::~TerarkBenchmark() {
    assert(tab == NULL);
}

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
  std::string key;
  setting.splitKeyValue(str, &key, NULL);
  return key;
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
    std::cout << "Read data from : " << setting.getLoadDataPath() << std::endl;
    std::string str;
    long long recordnumber = 0;
    const Schema& rowSchema = tab->rowSchema();
    const Schema& keySchema = tab->getIndexSchema(0);
    valvec<byte_t> row;
    DbContextPtr ctx = tab->createDbContext();
    ctx->syncIndex = setting.FLAGS_sync_index;
    terark::Auto_fclose loadFile(fopen(setting.getLoadDataPath().c_str(), "r"));
    if (!loadFile) {
      fprintf(stderr, "ERROR: fopen(%s, r) = %s\n",
          setting.getLoadDataPath().c_str(), strerror(errno));
      exit(1);
    }
    terark::LineBuf line;
    terark::valvec<byte_t> key;
    std::string strKey;
    size_t colnum = rowSchema.columnNum();
    terark::db::ColumnVec cols;
    while (line.getline(loadFile) > 0) {
        if (rowSchema.parseDelimText('\t', line, &row) != colnum) {
            std::cerr << "ERROR STR:" << str << std::endl;
            continue;
        }
        llong rid = ctx->upsertRow(row);
        if (rid < 0) { // unique index
            printf("Insert failed: %s\n", ctx->errMsg.c_str());
        }
        assert(VerifyOneKey(rid, row, ctx) == true);
        rowSchema.parseRow(row, &cols);
        keySchema.selectParent(cols, &key);
        strKey.assign((char*)key.data(), key.size());
        pushKey(strKey);
        recordnumber++;
        if (recordnumber % 100000 == 0)
            std::cout << "Insert reocord number: " << recordnumber / 10000 << "w" << std::endl;
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
    if (!getRandomKey(rkey, thread->randGenerator)) {
        std::cout << "allkeys empty" << std::endl;
        return false;
    }
    tab->indexSearchExact(indexId, rkey, &(thread->idvec), thread->ctx.get());
    //assert(idvec.size() <= 1);
    if (thread->idvec.size() == 0){        
            // std::cerr << "read error: keys not exist ! key = " << rkey << std::endl;
            return false;
    }
    thread->ctx->getValue(thread->idvec[0], &(thread->row));
    return true;
}

bool TerarkBenchmark::UpdateOneKey(ThreadState *thread) {
    std::string &rkey = thread->key;
    if (!getRandomKey(rkey, thread->randGenerator)) {
        std::cout << "allkeys empty" << std::endl;
        return false;
    }
    fstring key(rkey);
    tab->indexSearchExact(indexId, key, &(thread->idvec), thread->ctx.get());
    //assert(idvec.size() <= 1);
    if (thread->idvec.size() == 0) {
        // std::cerr << "update error: keys not exist ! key = " << rkey << std::endl;
        return false;
    }
    thread->ctx->getValue(thread->idvec[0], &(thread->row));
    try {
        llong rid = thread->ctx->upsertRow(thread->row);
        if (rid < 0) { // unique index
            //printf("Update failed: %s\n", thread->ctx->errMsg.c_str());
            return false;
        }
    } catch (const std::exception &e) {
        //std::cerr << "update error :" << e.what() << std::endl;
        return false;
    }
    return true;
}

bool TerarkBenchmark::InsertOneKey(ThreadState *thread) {
    static const Schema &rowSchema = tab->rowSchema();
    std::string &rstr = thread->str;
    if (!updateDataCq.try_pop(rstr)) {
        //std::cerr << "cq empty" << std::endl;
        return false;
    }
    if (rowSchema.columnNum() != rowSchema.parseDelimText('\t', rstr, &(thread->row))) {
        //std::cerr << "InsertOneKey error:" << rstr << std::endl;
        return false;
    }
    try {
        if (thread->ctx->upsertRow(thread->row) < 0) { // unique index
            //printf("Insert failed: %s\n", thread->ctx->errMsg.c_str());
            return false;
        }
    } 
    catch (const std::exception &e) {
        //std::cerr << "insert error:" << e.what() << std::endl;
        return false;
    }
    thread->key = getKey(rstr);
    pushKey(thread->key);
    return true;
}

bool TerarkBenchmark::Compact() {
    tab->compact();
    return true;
}

std::string TerarkBenchmark::HandleMessage(const std::string &msg) {
    std::stringstream ss;
    if (msg.empty())
        return ss.str();
    static const std::unordered_map<std::string,
            std::pair<
                bool (TerarkBenchmark::*)(const std::string &),
                std::string (TerarkBenchmark::*)(void)
            >
    > handleFuncMap = {
        {"write_throttle", { &TerarkBenchmark::updateWriteThrottle,
                             &TerarkBenchmark::getWriteThrottle }
        },
        {"colgroup_mmapPopulate", { &TerarkBenchmark::updateColGroupMmapPopulate,
                                    &TerarkBenchmark::getColGroupMmapPopulate}
        },
        {"index_mmappopulate", {&TerarkBenchmark::updateIndexMmapPopulate,
                                &TerarkBenchmark::getIndexMmapPopulate}
        },
        {"checksumLevel", {&TerarkBenchmark::updateCheckSumLevel,
                           &TerarkBenchmark::getCheckSumLevel}
        },
        {"dictziosampleratio", {&TerarkBenchmark::updateDictZipSampleRatio,
                                &TerarkBenchmark::getDictZipSampleRatio}
        },
        {"minmergesegnum", {&TerarkBenchmark::updateMinMergeSetNum,
                            &TerarkBenchmark::getMinMergeSetNum}
        },
        {"purgedeletethreshold", {&TerarkBenchmark::updatePurgeDeleteThreshold,
                                  &TerarkBenchmark::getPurgeDeleteThreshold}
        },
        {"maxwritingsegmentsize", {&TerarkBenchmark::updateMaxWritingSegmentSize,
                                   &TerarkBenchmark::getMaxWritingSegmentSize}
        },
    };
    size_t div = msg.find(':');
    if (div == std::string::npos)
        return ss.str();
    std::string key = msg.substr(0, div);
    std::string value = msg.substr(div + 1);
    auto iter = handleFuncMap.find(key);
    if (handleFuncMap.end() != iter) {
        auto updateConf = iter->second.first;
        (this->*updateConf)(value);
    }
    for (const auto& each : handleFuncMap) {
        auto getConf = each.second.second;
        ss << each.first << ":" << (this->*getConf)() << std::endl;
    }
    puts(ss.str().c_str());
    return ss.str();
}

bool TerarkBenchmark::updateWriteThrottle(const std::string &val) {
    char *ch;
    size_t bytes = strtol(val.c_str(), &ch, 10);
    if (*ch == 'K' || *ch == 'k') {
        bytes = bytes * 1024;
    } else if (*ch == 'M' || *ch == 'm') {
        bytes = bytes * 1024 * 1024;
    }
    tab->getSchemaConfig().m_writeThrottleBytesPerSecond = bytes;
    return true;
}

bool TerarkBenchmark::updateColGroupMmapPopulate(const std::string &val) {
    if (val == "true")
        tab->getColgroupSchemaForChange(colgroupId).m_mmapPopulate = true;
    else if (val == "false")
        tab->getColgroupSchemaForChange(colgroupId).m_mmapPopulate = false;
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
    float value;
    if (1 != sscanf(val.c_str(), "%f", &value))
        return false;
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
    ss << tab->getIndexSchemaForChange(indexId).m_mmapPopulate;
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
    if (sscanf(val.c_str(), "%lf", &value) != 1)
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

ThreadState *TerarkBenchmark::newThreadState(std::atomic<std::vector<bool > *> *whichSPlan) {
    return new ThreadState(threads.size(), nullptr, whichSPlan, &tab);
}

