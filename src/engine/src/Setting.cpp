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

std::string BaseSetting::BenchmarkName;
void Setting::wiredTigerSetting(int argc, char **argv){
    std::string default_db_path;
    for (int i = 2; i < argc; i++) {
        double d;
        int n;
        long size;
        char junk;
        if (leveldb::Slice(argv[i]).starts_with("--benchmarks=")) {
            FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
        } else if (sscanf(argv[i], "--compression_ratio=%lf%c", &d, &junk) == 1) {
            FLAGS_compression_ratio = d;
        } else if (sscanf(argv[i], "--histogram=%d%c", &n, &junk) == 1 &&
                   (n == 0 || n == 1)) {
            FLAGS_histogram = n;
        } else if (sscanf(argv[i], "--use_lsm=%d%c", &n, &junk) == 1 &&
                   (n == 0 || n == 1)) {
            FLAGS_use_lsm = n;
        } else if (sscanf(argv[i], "--use_existing_db=%d%c", &n, &junk) == 1 &&
                   (n == 0 || n == 1)) {
            FLAGS_use_existing_db = n;
        } else if (sscanf(argv[i], "--max_compact_wait=%d%c", &n, &junk) == 1) {
            FLAGS_max_compact_wait = n;
        } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
            FLAGS_num = n;
        } else if (sscanf(argv[i], "--reads=%d%c", &n, &junk) == 1) {
            FLAGS_reads = n;
        } else if (sscanf(argv[i], "--stagger=%d%c", &n, &junk) == 1 &&
                   (n == 0 || n == 1)) {
            FLAGS_stagger = n;
        } else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
            FLAGS_threads = n;
        } else if (sscanf(argv[i], "--value_size=%d%c", &n, &junk) == 1) {
            FLAGS_value_size = n;
        } else if (sscanf(argv[i], "--write_buffer_size=%d%c", &n, &junk) == 1) {
            FLAGS_write_buffer_size = n;
        } else if (sscanf(argv[i], "--cache_size=%ld%c", &size, &junk) == 1) {

            FLAGS_cache_size = size;
            std::cout << "cache_size " << FLAGS_cache_size << std::endl;
        } else if (sscanf(argv[i], "--bloom_bits=%d%c", &n, &junk) == 1) {
            FLAGS_bloom_bits = n;
        } else if (sscanf(argv[i], "--open_files=%d%c", &n, &junk) == 1) {
            FLAGS_open_files = n;
        } else if (sscanf(argv[i], "--read_ratio=%lf%c", &d, &junk) == 1) {
            FLAGS_read_write_percent = d;
        } else if (strncmp(argv[i], "--resource_data=", 16) == 0) {
            FLAGS_resource_data = argv[i] + 16;
        } else if (strncmp(argv[i], "--keys_data=", 12) == 0){
            FLAGS_keys_data = argv[i] + 12;
            std::cout << "FLAGS_keys_data:" << FLAGS_keys_data << std::endl;
        }
    }

    // Choose a location for the test database if none given with --db=<path>
    if (FLAGS_db == NULL) {
        leveldb::Env::Default()->GetTestDirectory(&default_db_path);
        default_db_path += "/dbbench";
        FLAGS_db = default_db_path.c_str();
        dbdirs = {FLAGS_db};
    }
}

void Setting::terarkSetting(int argc, char **argv) {
    FLAGS_write_buffer_size = leveldb::Options().write_buffer_size;
    FLAGS_open_files = leveldb::Options().max_open_files;
    std::string default_db_path;
    std::string default_db_table;
    for (int i = 2; i < argc; i++) {
        double d;
        int n;
        char junk;
        if (leveldb::Slice(argv[i]).starts_with("--benchmarks=")) {
            FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
        } else if (sscanf(argv[i], "--compression_ratio=%lf%c", &d, &junk) == 1) {
            FLAGS_compression_ratio = d;
        } else if (sscanf(argv[i], "--histogram=%d%c", &n, &junk) == 1 &&
                   (n == 0 || n == 1)) {
            FLAGS_histogram = n;
        } else if (sscanf(argv[i], "--use_existing_db=%d%c", &n, &junk) == 1 &&
                   (n == 0 || n == 1)) {
            FLAGS_use_existing_db = n;
        } else if (sscanf(argv[i], "--sync_index=%d%c", &n, &junk) == 1 &&
                   (n == 0 || n == 1)) {
            FLAGS_sync_index = n;
        } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
            FLAGS_num = n;
        } else if (sscanf(argv[i], "--reads=%d%c", &n, &junk) == 1) {
            FLAGS_reads = n;
        } else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
            FLAGS_threads = n;
        } else if (sscanf(argv[i], "--read_ratio=%lf%c", &d, &junk) == 1) {
            FLAGS_read_write_percent = d;
        } else if (sscanf(argv[i], "--read_old_ratio=%lf%c", &d, &junk) == 1) {
            FLAGS_read_old_record_percent = d;
        } else if (sscanf(argv[i], "--write_ratio=%lf%c", &d, &junk) == 1) {
            FLAGS_write_new_record_percent = d;
        } else if (strncmp(argv[i], "--resource_data=", 16) == 0) {
            FLAGS_resource_data = argv[i] + 16;
            std::cout << "FLAGSresource_data:" << FLAGS_resource_data << std::endl;
        } else if (strncmp(argv[i], "--keys_data=", 12) == 0){
            FLAGS_keys_data = argv[i] + 12;
            std::cout << "FLAGS_keys_data:" << FLAGS_keys_data << std::endl;
        }
    }

    // Choose a location for the test database if none given with --db=<path>
    if (FLAGS_db == NULL) {
        leveldb::Env::Default()->GetTestDirectory(&default_db_path);
        default_db_path += "/dbbench";
        FLAGS_db = default_db_path.c_str();
        dbdirs = {FLAGS_db};
    }

    if (FLAGS_db_table.size() == 0) {
        default_db_table += "DfaDbTable";
        FLAGS_db_table = default_db_table;
    }
}

Setting::Setting(int argc,char **argv,char *name){
    using namespace terark;
    FLAGS_block_size = 16 * 1024;
    FLAGS_min_level_to_compress = 1;

    for (int i = 2; i < argc; ++i) {
        fstring arg = argv[i];
        if (arg.startsWith("--keyfields=")) {
            valvec<fstring> fields;
            arg.substr(strlen("--keyfields=")).split(',', &fields);
            for(size_t j = 0; j < fields.size(); ++j) {
                keyFields.push_back(lcast(fields[j]));
            }
        }
        else if (arg.startsWith("--numfields=")) {
            numFields = lcast(arg.substr(strlen("--numfields=")));
        }
        else if (arg.startsWith("--fieldsDelim=")) {
            fieldsDelim = arg[strlen("--fieldsDelim=")];
        }
        else if (arg.startsWith("--keySampleRatio=")) {
            keySampleRatio = lcast(arg.substr(strlen("--keySampleRatio=")));
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
        else if (arg.startsWith("--skipInsertLines=")) {
            skipInsertLines = lcast(arg.substr(strlen("--skipInsertLines=")));
        }
        else if (arg.startsWith("--alt_engine_name=")) {
            alt_engine_name = arg.substr(strlen("--alt_engine_name=")).str();
        }
        else if (arg.startsWith("--disable_wal") || arg.startsWith("--disableWAL")) {
            disableWAL = true;
            printf("disableWAL = true\n");
        }
    }
    dbdirs = {FLAGS_db};
    if (size_t(-1) == numFields) {
        fprintf(stderr, "ERROR: missing argument --numfields=...\n");
        exit(1);
    }
    if (keyFields.empty()) {
        fprintf(stderr, "ERROR: missing argument --keyfields=...\n");
        exit(1);
    }
    if ( strcmp(name,"wiredtiger") == 0){
        wiredTigerSetting(argc,argv);
    }else if (strcmp(name,"terarkdb") == 0){
        terarkSetting(argc,argv);
    }else if (strcmp(name,"compact") == 0){
        terarkSetting(argc,argv);
    } else if (strcmp(name, "rocksdb") == 0 || strcmp(name, "terark_rocksdb") == 0)
        rocksdbSetting(argc, argv);
    else if (strcmp(name, "posix") == 0) {
        terarkSetting(argc,argv);
    }
    else {
        fprintf(stderr,"error:argv[1]:%s",argv[1]);
        exit(1);
    }
    setThreadNums(FLAGS_threads);
    setBaseSetting(argc,argv);
    BaseSetting::BenchmarkName.assign(name);
    assert(FLAGS_db != nullptr);
    std::cout << toString() << std::endl;
    std::cout << "wait" << std::endl;
}

void Setting::rocksdbSetting(int argc, char **argv) {
    for (int i = 2; i < argc; i++) {
        double d;
        long n;
        char junk;
        terark::fstring arg(argv[i]);
        if (arg.startsWith("--benchmarks=")) {
            FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
        } else if (sscanf(argv[i], "--compression_ratio=%lf%c", &d, &junk) == 1) {
            FLAGS_compression_ratio = d;
        } else if (sscanf(argv[i], "--histogram=%ld%c", &n, &junk) == 1 &&
                   (n == 0 || n == 1)) {
            FLAGS_histogram = n;
        } else if (sscanf(argv[i], "--use_existing_db=%ld%c", &n, &junk) == 1 &&
                   (n == 0 || n == 1)) {
            FLAGS_use_existing_db = n;
        } else if (sscanf(argv[i], "--num=%ld%c", &n, &junk) == 1) {
            FLAGS_num = n;
        } else if (sscanf(argv[i], "--reads=%ld%c", &n, &junk) == 1) {
            FLAGS_reads = n;
        } else if (sscanf(argv[i], "--threads=%ld%c", &n, &junk) == 1) {
            FLAGS_threads = n;
        } else if (sscanf(argv[i], "--value_size=%ld%c", &n, &junk) == 1) {
            FLAGS_value_size = n;
        } else if (sscanf(argv[i], "--write_buffer_size=%ld%c", &n, &junk) == 1) {
            if ('k' == junk || 'K' == junk)
                FLAGS_write_buffer_size = n << 10;
            else if ('m' == junk || 'M' == junk)
                FLAGS_write_buffer_size = n << 20;
            else if ('g' == junk || 'G' == junk)
                FLAGS_write_buffer_size = n << 30;
            else
                FLAGS_write_buffer_size = n;
            std::cout << "FLAGS_write_buffer_size " << FLAGS_write_buffer_size << std::endl;
        } else if (sscanf(argv[i], "--cache_size=%ld%c", &n, &junk) == 1) {
            FLAGS_cache_size = n;
            std::cout << " cache size " << FLAGS_cache_size << std::endl;
        } else if (sscanf(argv[i], "--bloom_bits=%ld%c", &n, &junk) == 1) {
            FLAGS_bloom_bits = n;
        } else if (sscanf(argv[i], "--open_files=%ld%c", &n, &junk) == 1) {
            FLAGS_open_files = n;
        } else if (strncmp(argv[i], "--resource_data=", 16) == 0) {
            FLAGS_resource_data = argv[i] + 16;
        } else if (arg.startsWith("--flushThreads=")) {
            flushThreads = terark::lcast(arg.substr(strlen("--flushThreads=")));
        } else if (arg.startsWith("--compactThreads=")) {
            compactThreads = terark::lcast(arg.substr(strlen("--compactThreads=")));
        } else if (arg.startsWith("--numLevels=")) {
            FLAGS_num_levels = terark::lcast(arg.substr(strlen("--numLevels=")));
        } else if (arg.startsWith("--target_file_size_multiplier=")) {
            target_file_size_multiplier = terark::lcast(arg.substr(strlen("--target_file_size_multiplier=")));
        } else if (arg.startsWith("--universalCompaction=")) {
            rocksdbUniversalCompaction = (int)terark::lcast(arg.substr(strlen("--universalCompaction=")));
        }
    }
}

BaseSetting::BaseSetting() :
        setFuncMap{
                {"-stop"              , &BaseSetting::strSetStop },
                {"-thread_num"        , &BaseSetting::strSetThreadNums},
                {"-sampling_rate"     , &BaseSetting::strSetSamplingRate},
                {"-insert_data_path"  , &BaseSetting::strSetInsertDataPath},
                {"-load_data_path"    , &BaseSetting::strSetLoadDataPath},
                {"-load_or_run"       , &BaseSetting::strSetLoadOrRun},
                {"-keys_data_path"    , &BaseSetting::strSetKeysDataPath},
                {"-compact"           , &BaseSetting::strSetCompactTimes},
                {"-message"           , &BaseSetting::strSetMessage},
                {"-plan_config"       , &BaseSetting::strSetPlanConfigs},
                {"-thread_plan_map"   , &BaseSetting::strSetThreadPlan},
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
    ret << "load_or_run:\t"     << action << std::endl;
    ret << "keys_data_path:\t"  << getKeysDataPath() << std::endl;
    ret << "insert_data_path:\t"<< getInsertDataPath() << std::endl;
    ret << "load_data_path:\t"  << getLoadDataPath() << std::endl;
    ret << "compact times:\t"   << static_cast<int >(getCompactTimes()) << std::endl;
    ret << "message from " << BenchmarkName << std::endl;
    std::string msg;
    while (!response_message_cq.empty()) {
        if (true == response_message_cq.try_pop(msg))
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
    if (message_cq.try_pop(str) == true)
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
    const char split_ch = ':';
    std::stringstream ss(val);
    uint32_t thread_id,plan_id;
    ss >> thread_id;
    if (threadPlanMap.count(thread_id) == 0)
        return false;
    if (ss.get() != split_ch)
        return false;
    ss >> plan_id;
    {
        std::lock_guard<std::mutex> _lock(planMtx);
        if (plan_id >= planConfigs.size())
            return false;
    }
    threadPlanMap[thread_id] = plan_id;
    return true;
}
