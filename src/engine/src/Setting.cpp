//
// Created by terark on 16-8-1.
//

#include "Setting.h"
#include <include/leveldb/env.h>
#include <include/leveldb/options.h>
#include <rocksdb/env.h>
#include <terark/lcast.hpp>
#include <terark/valvec.hpp>
#include <sstream>
#include <iomanip>

using terark::fstring;
using terark::lcast;
using terark::ParseSizeXiB;

std::string BaseSetting::BenchmarkName;

Setting::Setting(int argc, char **argv) {
    using namespace terark;
    FLAGS_block_size = 16 * 1024;
    FLAGS_min_level_to_compress = 1;

    for (int i = 1; i < argc; ++i) {
        double d;
        char junk;
        long n;
        fstring arg = argv[i];
        if (arg.startsWith("--key_fields=")) {
            valvec<fstring> fields;
            arg.substr(strlen("--key_fields=")).split(',', &fields);
            for(size_t j = 0; j < fields.size(); ++j) {
                keyFields.push_back(lcast(fields[j]));
            }
        }
        else if (arg.startsWith("--fields_num=")) {
            numFields = lcast(arg.substr(strlen("--fields_num=")));
        }
        else if (arg.startsWith("--fields_delim=")) {
            fieldsDelim = arg[strlen("--fields_delim=")];
        }
        else if (arg.startsWith("--logdir=")) {
            logdir = arg.substr(strlen("--logdir=")).str();
        }
        else if (arg.startsWith("--waldir=")) {
            waldir = arg.substr(strlen("--waldir=")).str();
        }
        else if (arg.startsWith("--db=")) {
            FLAGS_db = arg.substr(strlen("--db=")).str();
        }
        else if (arg.startsWith("--skip_insert_lines=")) {
            skipInsertLines = lcast(arg.substr(strlen("--skip_insert_lines=")));
        }
        else if (arg.startsWith("--alt_engine_name=")) {
            alt_engine_name = arg.substr(strlen("--alt_engine_name=")).str();
        }
        else if (arg.startsWith("--disable_wal") || arg.startsWith("--disableWAL")) {
            disableWAL = true;
            printf("disable_wal = true\n");
        }
        else if (arg.startsWith("--sync_index=")) {
            FLAGS_sync_index = (int)lcast(arg.substr(strlen("--sync_index=")));
        }
        else if (arg.startsWith("--benchmarks=")) {
            FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
        }
        else if (sscanf(argv[i], "--compression_ratio=%lf%c", &d, &junk) == 1) {
            FLAGS_compression_ratio = d;
        }
        else if (sscanf(argv[i], "--histogram=%ld%c", &n, &junk) == 1 &&
                   (n == 0 || n == 1)) {
            FLAGS_histogram = n;
        }
        else if (sscanf(argv[i], "--use_lsm=%ld%c", &n, &junk) == 1 &&
                 (n == 0 || n == 1)) {
            FLAGS_use_lsm = n;
        }
        else if (sscanf(argv[i], "--use_existing_db=%ld%c", &n, &junk) == 1 &&
                   (n == 0 || n == 1)) {
            FLAGS_use_existing_db = n;
        }
        else if (sscanf(argv[i], "--num=%ld%c", &n, &junk) == 1) {
            FLAGS_num = n;
        }
        else if (sscanf(argv[i], "--reads=%ld%c", &n, &junk) == 1) {
            FLAGS_reads = n;
        }
        else if (sscanf(argv[i], "--threads=%ld%c", &n, &junk) == 1) {
            FLAGS_threads = n;
        }
        else if (sscanf(argv[i], "--value_size=%ld%c", &n, &junk) == 1) {
            FLAGS_value_size = n;
        }
        else if (arg.startsWith("--write_buffer_size=")) {
            FLAGS_write_buffer_size = ParseSizeXiB(arg.p + strlen("--write_buffer_size="));
        }
        else if (arg.startsWith("--block_size=")) {
            FLAGS_block_size = ParseSizeXiB(arg.p + strlen("--block_size="));
        }
        else if (arg.startsWith("--cache_size=")) {
            FLAGS_cache_size = ParseSizeXiB(arg.p + strlen("--cache_size="));
        }
        else if (sscanf(argv[i], "--bloom_bits=%ld%c", &n, &junk) == 1) {
            FLAGS_bloom_bits = n;
        }
        else if (sscanf(argv[i], "--open_files=%ld%c", &n, &junk) == 1) {
            FLAGS_open_files = n;
        }
        else if (arg.startsWith("--resource_data=")) {
            FLAGS_resource_data = arg.substr(strlen("--resource_data=")).c_str();
        }
        else if (arg.startsWith("--flush_threads=")) {
            flushThreads = lcast(arg.substr(strlen("--flush_threads=")));
        }
        else if (arg.startsWith("--compact_threads=")) {
            compactThreads = lcast(arg.substr(strlen("--compact_threads=")));
        }
        else if (arg.startsWith("--num_levels=")) {
            FLAGS_num_levels = lcast(arg.substr(strlen("--num_levels=")));
        }
        else if (arg.startsWith("--target_file_size_multiplier=")) {
            target_file_size_multiplier = lcast(arg.substr(strlen("--target_file_size_multiplier=")));
        }
        else if (arg.startsWith("--terocksdb_tmpdir=")) {
            terocksdb_tmpdir = arg.substr(strlen("--terocksdb_tmpdir=")).c_str();
        }
         else if (arg.startsWith("--use_universal_compaction=")) {
            rocksdbUniversalCompaction = lcast(arg.substr(strlen("--use_universal_compaction=")));
        }
        else if (arg.startsWith("--write_rate_limit=")) {
            write_rate_limit = ParseSizeXiB(arg.substr(strlen("--write_rate_limit=")));
        }
        else if (arg.startsWith("--auto_slowdown_write=")) {
            autoSlowDownWrite = lcast(arg.substr(strlen("--auto_slowdown_write=")));
        }
        else if (arg.startsWith("--index_nest_level=")) {
            terocksdbIndexNestLevel = lcast(arg.substr(strlen("--index_nest_level=")));
        }
        else if (arg.startsWith("--zip_work_mem_soft_limit=")) {
            terocksdbZipWorkingMemSoftLimit = ParseSizeXiB(arg.substr(strlen("--zip_work_mem_soft_limit=")));
        }
        else if (arg.startsWith("--zip_work_mem_hard_limit=")) {
            terocksdbZipWorkingMemHardLimit = ParseSizeXiB(arg.substr(strlen("--zip_work_mem_hard_limit=")));
        }
        else if (arg.startsWith("--small_task_mem=")) {
            terocksdbSmallTaskMemory = ParseSizeXiB(arg.substr(strlen("--small_task_mem=")));
        }
        else if (arg.startsWith("--checksum_level=")) {
            checksumLevel = lcast(arg.substr(strlen("--checksum_level=")));
        }
        else if (arg.startsWith("--terocksdb_sample_ratio=")) {
            terocksdbSampleRatio = lcast(arg.substr(strlen("--terocksdb_sample_ratio=")));
        }
        else if (arg.startsWith("--terocksdb_zip_min_level=")) {
            terocksdbZipMinLevel = lcast(arg.substr(strlen("--terocksdb_zip_min_level=")));
        }
        else if (arg.startsWith("--index_cache_ratio=")) {
            terocksdbIndexCacheRatio = lcast(arg.substr(strlen("--index_cache_ratio=")));
        }
        else if (arg.startsWith("--enable_auto_compact=")) {
            FLAGS_enable_auto_compact = lcast(arg.substr(strlen("--enable_auto_compact=")));
        }
        else if (arg.startsWith("--rocksdb_memtable=")) {
            FLAGS_rocksdb_memtable = arg.p + strlen("--rocksdb_memtable=");
        }
        else if (arg.startsWith("--load_size=")) {
            FLAGS_load_size = ParseSizeXiB(arg.substr(strlen("--load_size=")));
        }
        else if (arg.startsWith("--use_mmap=")) {
          FLAGS_use_mmap = lcast(arg.substr(strlen("--use_mmap=")));
        }
        else if (arg.startsWith("--mysql_passwd=")) {
            extern const char* g_passwd;
            g_passwd = arg.p + strlen("--mysql_passwd=");
        }
    }
    if (FLAGS_db.empty()) {
        fprintf(stderr, "ERROR: missing argument --db=...\n");
        exit(1);
    }
    dbdirs = {FLAGS_db};
    if (size_t(-1) == numFields) {
        fprintf(stderr, "ERROR: missing argument --fields_num=...\n");
        exit(1);
    }
    if (keyFields.empty()) {
        fprintf(stderr, "ERROR: missing argument --key_fields=...\n");
        exit(1);
    }
    keyFieldsBits.resize_fill(numFields, false);
    for (size_t i = 0; i < keyFields.size(); ++i) {
      size_t field = keyFields[i];
      if (field >= numFields) {
        fprintf(stderr, "ERROR: bad key field: %zd, num_fields = %zd\n", field, numFields);
        exit(1);
      }
      keyFieldsBits.set1(field);
    }
    setThreadNums(FLAGS_threads);
    setBaseSetting(argc, argv);
    assert(!FLAGS_db.empty());
    fprintf(stderr, "----%s\n%s----%s\n\n"
        , BOOST_CURRENT_FUNCTION
        , toString().c_str()
        , BOOST_CURRENT_FUNCTION
        );
}

size_t
Setting::splitKeyValue(fstring row, std::string* key, std::string* val) const {
  using namespace terark;
  thread_local valvec<fstring> strvec;
  strvec.erase_all();
  byte_t delim = byte_t(fieldsDelim);
  row.split(delim, &strvec);
  key->resize(0);
  if (val)
    val->resize(0);
  if (strvec.size() < numFields)
    return 0;
  for (size_t field: keyFields) {
    key->append(strvec[field].data(), strvec[field].size());
    key->push_back(delim);
  }
  key->pop_back();
  if (val) {
    size_t normFields = std::min(strvec.size(), numFields);
    auto isKeyField = keyFieldsBits.bldata();
    size_t i = 0;
    for (; i < normFields; i++) {
      if (!terark_bit_test(isKeyField, i)) {
        val->append(strvec[i].data(), strvec[i].size());
        val->push_back(delim);
      }
    }
    for (; i < strvec.size(); ++i) {
      val->append(strvec[i].data(), strvec[i].size());
      val->push_back(delim);
    }
    val->pop_back();
  }
  return strvec.size();
}


BaseSetting::BaseSetting() :
        planConfigs{
                {90, 5, 5}
        },
        setFuncMap{
                {"--stop"              , &BaseSetting::strSetStop },
                {"--thread_num"        , &BaseSetting::strSetThreadNums},
                {"--sampling_rate"     , &BaseSetting::strSetSamplingRate},
                {"--insert_data_path"  , &BaseSetting::strSetInsertDataPath},
                {"--load_data_path"    , &BaseSetting::strSetLoadDataPath},
                {"--action"            , &BaseSetting::setAction},
                {"--keys_data_path"    , &BaseSetting::strSetKeysDataPath},
                {"--verify_kv_file"    , &BaseSetting::strSetVerifyKvFile},
                {"--compact"           , &BaseSetting::strSetCompactTimes},
                {"--message"           , &BaseSetting::strSetMessage},
                {"--plan_config"       , &BaseSetting::strSetPlanConfigs},
                {"--thread_plan_map"   , &BaseSetting::strSetThreadPlan},
        }
{
    compactTimes = 0;
    threadNums = 8;
    samplingRate.store(20);
    stop.store(false);
    action = "run";
}

uint8_t BaseSetting::getSamplingRate(void) const {
    return samplingRate.load();
}
bool BaseSetting::strSetSamplingRate(std::string &value) {
    uint8_t sp = stoi(value);
    if (sp > 100)
        return false;
    samplingRate.store(sp);
    return true;
}
bool BaseSetting::strSetStop(std::string &value) {
    if (value == "true"){
        stop.store(true);
        return true;
    }else if (value == "false"){
        stop.store(false);
        return true;
    }else{
        return false;
    }
}

bool BaseSetting::strSetThreadNums(std::string &value) {
    uint32_t threadNums = stoi(value);
    setThreadNums(threadNums);
    return true;
}
bool BaseSetting::ifStop() const
{
    return stop.load();
}

void BaseSetting::setThreadNums(uint8_t num)
{
    fprintf(stderr, "BaseSetting::setThreadNums(): thread_num=%d\n", num);
    auto old_num = threadPlanMap.size();
    while(old_num < num){
        threadPlanMap[old_num++] = 0;
    }
    while(old_num > num){
        old_num--;
        threadPlanMap.unsafe_erase(old_num);
    }
    threadNums.store(num);
}
uint32_t BaseSetting::getThreadNums(void) const {
    return threadNums.load();
}
void BaseSetting::setStop(void){
    stop.store(true);
}

/**
 * 输出所有配置信息
 * @return
 */
std::string BaseSetting::toString() {
    std::stringstream ret;
    ret << "benchmark name:\t"  << BenchmarkName << std::endl;
    ret << "sampling rate:\t"   << static_cast<int >(getSamplingRate()) << std::endl;
    ret << "thread nums:\t"     << getThreadNums() << std::endl;
    {
        std::lock_guard<std::mutex>  _lock(planMtx);
        using namespace std;
        for(size_t i = 0; i < planConfigs.size(); i ++) {
            ret << "plan " << setw(2) << i << " read " << setw(3) << planConfigs[i].read_percent << " insert "\
                << setw(3) << planConfigs[i].insert_percent << " update " << planConfigs[i].update_percent << std::endl;
        }
        for(const auto &plan : threadPlanMap){
            ret << "thread " << setw(2) << plan.first << " execute plan " << setw(2) << plan.second << std::endl;
        }
    }
    ret << "stop:\t"            << ifStop() << std::endl;
    ret << "action:\t"          << action << std::endl;
    ret << "keys_data_path:\t"  << getKeysDataPath() << std::endl;
    ret << "insert_data_path:\t"<< getInsertDataPath() << std::endl;
    ret << "load_data_path:\t"  << getLoadDataPath() << std::endl;
    ret << "verify_kv_file:\t" << getVerifyKvFile() << std::endl;
    ret << "compact times:\t"   << static_cast<int >(getCompactTimes()) << std::endl;
    ret << "message from " << BenchmarkName << std::endl;
    std::string msg;
    while (!response_message_cq.empty()) {
        if (response_message_cq.try_pop(msg))
            ret << msg << std::endl;
    }
    return ret.str();
}

/**
 * 设置基础配置
 * @param line
 * @return
 */
std::string BaseSetting::setBaseSetting(std::string &line) {
    std::vector<std::string> strvec;
    fstring(line).split(' ', &strvec);
    std::string message;
    if (strvec.size() == 0)
        message += "Empty Input\n";

    for(auto& each_kv : strvec){
        //key=value
        size_t div = each_kv.find('=');
        std::string key = each_kv.substr(0,div);
        std::string value = each_kv.substr(div+1);
        auto iter = setFuncMap.find(key);
        if (setFuncMap.end() != iter) {
            if ((this->*iter->second)(value)) {
                message += "set\t" + key + "\tsuccess\n";
            }
            else {
                message += "set\t" + key + "\tfailure\n";
            }
        }else{
            message += "invalid command:" + key + "\n";
        }
    }
    return message + toString();
}

std::string BaseSetting::setBaseSetting(int argc, char **argv) {
    std::string message;
    for(int i = 1; i < argc; i ++){
        char *pos = strchr(argv[i],'=');
        if (pos == NULL)
            continue;
        std::string key(argv[i],pos-argv[i]);
        std::string value(pos + 1);
        auto iter = setFuncMap.find(key);
        if (setFuncMap.end() == iter) {
            message += "invalid command:" + key + "\n";
            continue;
        }
        if ((this->*iter->second)(value)){
            message += "set\t" + key + "\tsuccess\t";
        }
        else{
            message += "set\t" + key + "\tfailure\t";
        }
    }
    return message;
}

bool BaseSetting::strSetInsertDataPath(std::string &value) {
    if (0 == value.size()){
        return false;
    }
    insertDataPath = value;
    return true;
}
const std::string &BaseSetting::getInsertDataPath(void) const {
    return insertDataPath;
}

const std::string &BaseSetting::getLoadDataPath(void) const {
    return loadDataPath;
}

bool BaseSetting::strSetLoadDataPath(std::string& value) {
    if (value.size() == 0)
        return false;
    loadDataPath = value;
    return true;
}

const std::string& BaseSetting::getAction(void) const {
    return action;
}

bool BaseSetting::setAction(std::string &value){
    action = value;
    return true;
}

const std::string &BaseSetting::getKeysDataPath(void) const {
    return keysDataPath;
}

bool BaseSetting::strSetKeysDataPath(std::string &value) {
    if(value.empty())
        return false;
    keysDataPath = value;
    return true;
}

const std::string &BaseSetting::getVerifyKvFile(void) const {
    return verifyKvFile;
}

bool BaseSetting::strSetVerifyKvFile(std::string &value) {
    if(value.empty())
        return false;
    verifyKvFile = value;
    return true;
}

uint8_t BaseSetting::getCompactTimes(void) const {
    return compactTimes.load();
}

bool BaseSetting::strSetCompactTimes(std::string &val) {
    compactTimes++;
    return true;
}

bool BaseSetting::strSetMessage(std::string &msg) {
    message_cq.push(msg);
    return true;
}

std::string BaseSetting::getMessage(void) {
    std::string str;
    if (message_cq.try_pop(str))
        return str;
    return str;
}

void BaseSetting::sendMessageToSetting(const std::string &str) {
    response_message_cq.push(str);
}

bool BaseSetting::strSetPlanConfigs(std::string &val) {
    const char split_ch = ':';
    std::stringstream ss(val);
    uint32_t plan_id;
    ss >> plan_id;
    if (ss.get() != split_ch)
        return false;

    uint32_t read_percent;
    ss >> read_percent;
    if (ss.get() != split_ch)
        return false;

    uint32_t write_percent;
    ss >> write_percent;
    if (ss.get() != split_ch)
        return false;

    uint32_t update_percent;
    ss >> update_percent;
    {
        std::lock_guard<std::mutex> _lock(planMtx);
        if (planConfigs.size() <= plan_id) {
            planConfigs.resize(plan_id + 1);
        }
        planConfigs[plan_id].read_percent = read_percent;
        planConfigs[plan_id].insert_percent = write_percent;
        planConfigs[plan_id].update_percent = update_percent;
    }
    return true;
}

bool BaseSetting::getPlanConfig(const uint32_t thread_id, PlanConfig &planConfig) {
    auto iter = threadPlanMap.find(thread_id);
    if (iter == threadPlanMap.end())
        return false;
    auto plan_id = threadPlanMap[thread_id];
    {
        std::lock_guard<std::mutex> _lock(planMtx);
        if (plan_id >= planConfigs.size())
            return false;
        planConfig = planConfigs[plan_id];
    }
    return true;
}

bool BaseSetting::strSetThreadPlan(std::string &val) {
    printf("BaseSetting::strSetThreadPlan(%s)\n", val.c_str());
    char* endp = NULL;
    const char* str = val.c_str();
    auto thread_id_beg = strtoul(str, &endp, 10);
    auto thread_id_end = thread_id_beg;
    str = endp + 1;
    if ('-' == *endp) {
        thread_id_end = strtoul(str, &endp, 10);
    }
    str = endp + 1;
    if (':' != *endp) {
        return false;
    }
    auto plan_id = strtoul(str, &endp, 10);
    {
        std::lock_guard<std::mutex> _lock(planMtx);
        if (plan_id >= planConfigs.size())
            return false;
    }
    for (auto thread_id = thread_id_beg; thread_id <= thread_id_end; ++thread_id) {
        threadPlanMap[uint32_t(thread_id)] = uint32_t(plan_id);
    }
    return true;
}
