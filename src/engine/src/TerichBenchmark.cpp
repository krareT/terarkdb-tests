//
// Created by terark on 8/24/16.
//

#include "TerichBenchmark.h"
#include <terark/util/autoclose.hpp>
#include <terark/util/linebuf.hpp>
#include <fstream>

struct Terich_ThreadState : public ThreadState {
  Terich_ThreadState(int index, const std::atomic<std::vector<bool>*>* wsp,
                     const terark::terichdb::DbTablePtr& tab)
    : ThreadState(index, wsp) {
      ctx = tab->createDbContext();
      ctx->syncIndex = true;
  }
  terark::terichdb::DbContextPtr ctx;
  terark::valvec<terark::byte >  row;
  terark::valvec<terark::llong>  idvec;
};

TerichBenchmark::TerichBenchmark(Setting& setting1)
: Benchmark(setting1) {
  indexId = size_t(-1);
  colgroupId = size_t(-1);
}

TerichBenchmark::~TerichBenchmark() {
    assert(tab == NULL);
}

void TerichBenchmark::PrintHeader() {
    fprintf(stdout, "NarkDB Test Begins!");
}

void TerichBenchmark::Close() {
    clearThreads();
    tab->safeStopAndWaitForFlush();
    tab = NULL;
}

void TerichBenchmark::Load(void) {
    DoWrite(true);
    //tab->compact();
}

std::string TerichBenchmark::getKey(std::string &str) {
  std::string key;
  setting.splitKeyValue(str, &key, NULL);
  return key;
}

void TerichBenchmark::Open() {
    PrintHeader();
    assert(tab == NULL);
    fprintf(stderr, "Open database %s\n", setting.FLAGS_db.c_str());
    tab = CompositeTable::open(setting.FLAGS_db);
    assert(tab != NULL);
    indexId = tab->getIndexId("cur_title,cur_timestamp");
    assert(indexId < tab->getIndexNum());
    tab->setThrowOnThrottle(true);
    colgroupId = tab->getColgroupId("str34");
    assert(colgroupId < tab->getColumnNum());
}

void TerichBenchmark::DoWrite(bool seq) {
    fprintf(stderr, "Read data from : %s\n", setting.getLoadDataPath().c_str());
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
    terark::terichdb::ColumnVec cols;
    while (line.getline(loadFile) > 0) {
        if (rowSchema.parseDelimText('\t', line, &row) != colnum) {
            fprintf(stderr, "ERROR STR: %s\n", str.c_str());
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
        recordnumber++;
        if (recordnumber % 100000 == 0)
            fprintf(stderr, "Insert reocord number: %lldw\n", recordnumber / 10000);
    }
    time_t now;
    struct tm *timenow;
    time(&now);
    timenow = localtime(&now);
    printf("recordnumber %lld, time %s\n", recordnumber, asctime(timenow));
}

bool TerichBenchmark::VerifyOneKey(llong rid, valvec<byte> &outside, DbContextPtr &ctx) {
    valvec<byte> inside;
    ctx->getValue(rid, &inside);
    assert(inside.size() == outside.size());
    for (size_t i = 0; i < inside.size(); i++) {
        if (inside[i] != outside[i])
            return false;
    }
    return true;
}

bool TerichBenchmark::ReadOneKey(ThreadState *thread0) {
    auto thread = static_cast<Terich_ThreadState*>(thread0);
    std::string &rkey = thread->key;
    if (!getRandomKey(rkey, thread->randGenerator)) {
        fprintf(stderr, "TerarkBenchmark::ReadOneKey(): allkeys is empty\n");
        return false;
    }
    tab->indexSearchExact(indexId, rkey, &(thread->idvec), thread->ctx.get());
    //assert(idvec.size() <= 1);
    if (thread->idvec.size() == 0){
        return false;
    }
    thread->ctx->getValue(thread->idvec[0], &(thread->row));
    return true;
}

bool TerichBenchmark::UpdateOneKey(ThreadState *thread0) {
  auto thread = static_cast<Terich_ThreadState*>(thread0);
    std::string &rkey = thread->key;
    if (!getRandomKey(rkey, thread->randGenerator)) {
        fprintf(stderr, "TerarkBenchmark::ReadOneKey(): allkeys is empty\n");
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
            //printf("Update failed: %s\n", thread->ctx->errMsg.c_str());
            return false;
        }
    } catch (const std::exception &e) {
        return false;
    }
    return true;
}

bool TerichBenchmark::InsertOneKey(ThreadState *thread0) {
  auto thread = static_cast<Terich_ThreadState*>(thread0);
    static const Schema &rowSchema = tab->rowSchema();
    std::string &rstr = thread->str;
    if (!updateDataCq.try_pop(rstr)) {
        return false;
    }
    if (rowSchema.columnNum() != rowSchema.parseDelimText('\t', rstr, &(thread->row))) {
        return false;
    }
    try {
        if (thread->ctx->upsertRow(thread->row) < 0) { // unique index
            //printf("Insert failed: %s\n", thread->ctx->errMsg.c_str());
            return false;
        }
    } 
    catch (const std::exception &e) {
        return false;
    }
    thread->key = getKey(rstr);
    return true;
}

bool TerichBenchmark::Compact() {
    tab->compact();
    return true;
}

std::string TerichBenchmark::HandleMessage(const std::string &msg) {
    std::stringstream ss;
    if (msg.empty())
        return ss.str();
    static const std::unordered_map<std::string,
            std::pair<
                bool (TerichBenchmark::*)(const std::string &),
                std::string (TerichBenchmark::*)(void)
            >
    > handleFuncMap = {
        {"write_throttle", { &TerichBenchmark::updateWriteThrottle,
                             &TerichBenchmark::getWriteThrottle }
        },
        {"colgroup_mmapPopulate", { &TerichBenchmark::updateColGroupMmapPopulate,
                                    &TerichBenchmark::getColGroupMmapPopulate}
        },
        {"index_mmappopulate", {&TerichBenchmark::updateIndexMmapPopulate,
                                &TerichBenchmark::getIndexMmapPopulate}
        },
        {"checksumLevel", {&TerichBenchmark::updateCheckSumLevel,
                           &TerichBenchmark::getCheckSumLevel}
        },
        {"dictziosampleratio", {&TerichBenchmark::updateDictZipSampleRatio,
                                &TerichBenchmark::getDictZipSampleRatio}
        },
        {"minmergesegnum", {&TerichBenchmark::updateMinMergeSetNum,
                            &TerichBenchmark::getMinMergeSetNum}
        },
        {"purgedeletethreshold", {&TerichBenchmark::updatePurgeDeleteThreshold,
                                  &TerichBenchmark::getPurgeDeleteThreshold}
        },
        {"maxwritingsegmentsize", {&TerichBenchmark::updateMaxWritingSegmentSize,
                                   &TerichBenchmark::getMaxWritingSegmentSize}
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

bool TerichBenchmark::updateWriteThrottle(const std::string &val) {
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

bool TerichBenchmark::updateColGroupMmapPopulate(const std::string &val) {
    if (val == "true")
        tab->getColgroupSchemaForChange(colgroupId).m_mmapPopulate = true;
    else if (val == "false")
        tab->getColgroupSchemaForChange(colgroupId).m_mmapPopulate = false;
    else
        return false;
    return true;
}

bool TerichBenchmark::updateCheckSumLevel(const std::string &val) {
    int level = stoi(val);
    tab->getColgroupSchemaForChange(colgroupId).m_checksumLevel = level;
    return true;
}

bool TerichBenchmark::updateDictZipSampleRatio(const std::string &val) {
    float value;
    if (1 != sscanf(val.c_str(), "%f", &value))
        return false;
    tab->getColgroupSchemaForChange(colgroupId).m_dictZipSampleRatio = value;
    return true;
}

bool TerichBenchmark::updateIndexMmapPopulate(const std::string &val) {
    if (val == "true")
        tab->getIndexSchemaForChange(indexId).m_mmapPopulate = true;
    else if (val == "false")
        tab->getIndexSchemaForChange(indexId).m_mmapPopulate = false;
    else
        return false;
    return true;
}

std::string TerichBenchmark::getWriteThrottle(void) {
    std::stringstream ss;
    ss << tab->getSchemaConfig().m_writeThrottleBytesPerSecond;
    return ss.str();
}

std::string TerichBenchmark::getColGroupMmapPopulate(void) {
    std::stringstream ss;
    ss << tab->getColgroupSchemaForChange(colgroupId).m_mmapPopulate;
    return ss.str();
}

std::string TerichBenchmark::getIndexMmapPopulate(void) {
    std::stringstream ss;
    ss << tab->getIndexSchemaForChange(indexId).m_mmapPopulate;
    return ss.str();
}

std::string TerichBenchmark::getCheckSumLevel(void) {
    std::stringstream ss;
    ss << tab->getColgroupSchemaForChange(colgroupId).m_checksumLevel;
    return ss.str();
}

std::string TerichBenchmark::getDictZipSampleRatio(void) {
    std::stringstream ss;
    ss << tab->getColgroupSchemaForChange(colgroupId).m_dictZipSampleRatio;
    return ss.str();

}

bool TerichBenchmark::updateMinMergeSetNum(const std::string &val) {
    size_t value = stoi(val);
    tab->getSchemaConfig().m_minMergeSegNum = value;
    return true;
}

std::string TerichBenchmark::getMinMergeSetNum(void) {
    std::stringstream ss;
    ss << tab->getSchemaConfig().m_minMergeSegNum;
    return ss.str();
}

bool TerichBenchmark::updatePurgeDeleteThreshold(const std::string &val) {
    double value;
    if (sscanf(val.c_str(), "%lf", &value) != 1)
        return false;
    tab->getSchemaConfig().m_purgeDeleteThreshold = value;
    return true;
}

std::string TerichBenchmark::getPurgeDeleteThreshold(void) {
    std::stringstream ss;
    ss << tab->getSchemaConfig().m_purgeDeleteThreshold;
    return ss.str();
}

bool TerichBenchmark::updateMaxWritingSegmentSize(const std::string &val) {
    llong value = stoll(val);
    tab->getSchemaConfig().m_maxWritingSegmentSize = value;
    return true;
}

std::string TerichBenchmark::getMaxWritingSegmentSize(void) {
    std::stringstream ss;
    ss << tab->getSchemaConfig().m_maxWritingSegmentSize;
    return ss.str();
}

ThreadState* TerichBenchmark::newThreadState(const std::atomic<std::vector<bool>*> *whichSPlan) {
    return new Terich_ThreadState(threads.size(), whichSPlan, tab);
}

