//
// Created by terark on 16-8-1.
//

#include <include/leveldb/env.h>
#include "Setting.h"
#include <iostream>
#include <include/leveldb/options.h>
#include <sstream>
#include <rocksdb/env.h>
#include <terark/lcast.hpp>
#include <terark/valvec.hpp>

using terark::lcast;

std::string BaseSetting::BenchmarkName;

static uint64_t ParseSizeXiB(const char* str) {
    char* endp = NULL;
    double val = strtod(str, &endp);
    char scale = *endp;
    if ('k' == scale || 'K' == scale)
        return uint64_t(val * (1ull << 10));
    else if ('m' == scale || 'M' == scale)
        return uint64_t(val * (1ull << 20));
    else if ('g' == scale || 'G' == scale)
        return uint64_t(val * (1ull << 30));
    else
        return uint64_t(val);
}

Setting::Setting(int argc,char **argv) {
    using namespace terark;
    FLAGS_block_size = 16 * 1024;
    FLAGS_min_level_to_compress = 1;

    for (int i = 2; i < argc; ++i) {
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
        else if (arg.startsWith("--key_sample_ratio=")) {
            keySampleRatio = lcast(arg.substr(strlen("--key_sample_ratio=")));
            keySampleRatio = std::min(keySampleRatio, 1.0);
            keySampleRatio = std::max(keySampleRatio, 0.0001);
        }
        else if (arg.startsWith("--logdir=")) {
            logdir = arg.substr(strlen("--logdir=")).str();
        }
        else if (arg.startsWith("--waldir=")) {
            waldir = arg.substr(strlen("--waldir=")).str();
        }
        else if (arg.startsWith("--db=")) {
            FLAGS_db = argv[i] + 5;
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
        else if (sscanf(argv[i], "--use_lsm=%d%c", &n, &junk) == 1 &&
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
         else if (arg.startsWith("--universal_compaction=")) {
            rocksdbUniversalCompaction = lcast(arg.substr(strlen("--universal_compaction=")));
        }
        else if (arg.startsWith("--write_rate_limit=")) {
            write_rate_limit = lcast(arg.substr(strlen("--write_rate_limit=")));
        }
        else if (arg.startsWith("--auto_slowdown_write=")) {
            autoSlowDownWrite = lcast(arg.substr(strlen("--auto_slowdown_write=")));
        }
        else if (arg.startsWith("--mysql_passwd=")) {
			extern const char* g_passwd;
			g_passwd = arg.p + strlen("--mysql_passwd=");
        }
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
    setThreadNums(FLAGS_threads);
    setBaseSetting(argc,argv);
    BaseSetting::BenchmarkName.assign(argv[1]);
    assert(!FLAGS_db.empty());
    std::cout << toString() << std::endl;
    std::cout << "wait" << std::endl;
}

BaseSetting::BaseSetting() :
        setFuncMap{
                {"--stop"              , &BaseSetting::strSetStop },
                {"--thread_num"        , &BaseSetting::strSetThreadNums},
                {"--sampling_rate"     , &BaseSetting::strSetSamplingRate},
                {"--insert_data_path"  , &BaseSetting::strSetInsertDataPath},
                {"--load_data_path"    , &BaseSetting::strSetLoadDataPath},
                {"--action"            , &BaseSetting::strSetLoadOrRun},
                {"--keys_data_path"    , &BaseSetting::strSetKeysDataPath},
                {"--compact"           , &BaseSetting::strSetCompactTimes},
                {"--message"           , &BaseSetting::strSetMessage},
                {"--plan_config"       , &BaseSetting::strSetPlanConfigs},
                {"--thread_plan_map"   , &BaseSetting::strSetThreadPlan},
        },
        planConfigs{
                {90, 5, 5}
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
std::string BaseSetting::toString() {
    std::stringstream ret;
    ret << "benchmark name:\t"  << BaseSetting::BenchmarkName << std::endl;
    ret << "sampling rate:\t"   << static_cast<int >(getSamplingRate()) << std::endl;
    ret << "thread nums:\t"     << getThreadNums() << std::endl;
    {
        std::lock_guard<std::mutex>  _lock(planMtx);
        for(int i = 0; i < planConfigs.size(); i ++) {
            ret << "plan " << i << " read " << planConfigs[i].read_percent << " insert "\
                << planConfigs[i].insert_percent << " update " << planConfigs[i].update_percent << std::endl;
        }
        for(const auto &plan : threadPlanMap){
            ret << "thread " << plan.first << " execute plan " << plan.second << std::endl;
        }
    }
    ret << "stop:\t"            << ifStop() << std::endl;
    ret << "action:\t"          << action << std::endl;
    ret << "keys_data_path:\t"  << getKeysDataPath() << std::endl;
    ret << "insert_data_path:\t"<< getInsertDataPath() << std::endl;
    ret << "load_data_path:\t"  << getLoadDataPath() << std::endl;
    ret << "compact times:\t"   << static_cast<int >(getCompactTimes()) << std::endl;
    ret << "message from " << BenchmarkName << std::endl;
    std::string msg;
    while (!response_message_cq.empty()) {
        if (response_message_cq.try_pop(msg))
            ret << msg << std::endl;
    }
    return ret.str();
}
std::string BaseSetting::setBaseSetting(std::string &line) {
    std::vector<std::string> strvec;
    boost::split(strvec,line,boost::is_any_of(" \t"));
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
    for(int i = 0; i < argc; i ++){
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

std::string BaseSetting::ifRunOrLoad(void) const {
    return action;
}

bool BaseSetting::strSetLoadOrRun(std::string &value){
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
