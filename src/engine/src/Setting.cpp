//
// Created by terark on 16-8-1.
//

#include <include/leveldb/env.h>
#include "Setting.h"
#include <iostream>
#include <include/leveldb/options.h>
#include <sstream>
#include <rocksdb/env.h>

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
        } else if (strncmp(argv[i], "--db=", 5) == 0) {
            FLAGS_db = argv[i] + 5;
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
        } else if (strncmp(argv[i], "--db=", 5) == 0) {
            FLAGS_db = argv[i] + 5;
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
    }

    if (FLAGS_db_table.size() == 0) {
        default_db_table += "DfaDbTable";
        FLAGS_db_table = default_db_table;
    }




}

Setting::Setting(int argc,char **argv,char *name){
    if ( strcmp(name,"wiredtiger") == 0){
        wiredTigerSetting(argc,argv);
    }else if (strcmp(name,"terarkdb") == 0){
        terarkSetting(argc,argv);
    }else if (strcmp(name,"compact") == 0){
        terarkSetting(argc,argv);
    } else if (strcmp(name, "rocksdb") == 0 || strcmp(name, "terark_rocksdb") == 0)
        rocksdbSetting(argc, argv);
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
        // int n;
        long n;
        char junk;
        if (rocksdb::Slice(argv[i]).starts_with("--benchmarks=")) {
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
            FLAGS_write_buffer_size = n;
            std::cout << "FLAGS_write_buffer_size " << FLAGS_write_buffer_size << std::endl;
        } else if (sscanf(argv[i], "--cache_size=%ld%c", &n, &junk) == 1) {
            FLAGS_cache_size = n;
            std::cout << " cache size " << FLAGS_cache_size << std::endl;
        } else if (sscanf(argv[i], "--bloom_bits=%ld%c", &n, &junk) == 1) {
            FLAGS_bloom_bits = n;
        } else if (sscanf(argv[i], "--open_files=%ld%c", &n, &junk) == 1) {
            FLAGS_open_files = n;
        } else if (strncmp(argv[i], "--db=", 5) == 0) {
            FLAGS_db = argv[i] + 5;
        } else if (strncmp(argv[i], "--resource_data=", 16) == 0) {
            FLAGS_resource_data = argv[i] + 16;
        }
    }

}

BaseSetting::BaseSetting(){

    readPercent.store(80);
    insertPercent.store(10);
    samplingRate.store(20);
    stop.store(false);
    run = true;
    setFuncMap["-stop"]             = &BaseSetting::strSetStop;
    setFuncMap["-read_percent"]     = &BaseSetting::strSetReadPercent;
    setFuncMap["-thread_num"]       = &BaseSetting::strSetThreadNums;
    setFuncMap["-sampling_rate"]    = &BaseSetting::strSetSamplingRate;
    setFuncMap["-insert_data_path"] = &BaseSetting::strSetInsertDataPath;
    setFuncMap["-insert_percent"]   = &BaseSetting::strSetInsertPercent;
    setFuncMap["-load_data_path"]   = &BaseSetting::strSetLoadDataPath;
    setFuncMap["-load_or_run"]      = &BaseSetting::strSetLoadOrRun;
    setFuncMap["-keys_data_path"]   = &BaseSetting::strSetKeysDataPath;
    setFuncMap["-compact"]          = &BaseSetting::strSetCompactTimes;
    setFuncMap["-message"] = &BaseSetting::strSetMessage;
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
bool BaseSetting::strSetReadPercent(std::string& value) {

    uint32_t readPercent = stoi(value);

    if ( readPercent > MAX_READ_PERCNT)
        return false;
    setReadPercent(readPercent);
    return true;
}
bool BaseSetting::strSetThreadNums(std::string &value) {

    uint32_t threadNums = stoi(value);
    if (threadNums > MAX_THREAD_NUMS)
        return false;
    setThreadNums(threadNums);
    return true;
}
bool BaseSetting::ifStop() const
{
    return stop.load();
}
void BaseSetting::setReadPercent(uint8_t rp) {

    if ( rp > 100)
        rp = 100;
    readPercent.store(rp);
}

uint8_t BaseSetting::getReadPercent(void) const {

    return readPercent.load();
}
void BaseSetting::setThreadNums(uint32_t num)
{
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
    ret << "read percent:\t"    << static_cast<int >(getReadPercent()) << std::endl;
    ret << "sampling rate:\t"   << static_cast<int >(getSamplingRate()) << std::endl;
    ret << "thread nums:\t"     << getThreadNums() << std::endl;
    ret << "stop:\t"            << ifStop() << std::endl;
    ret << "insert percent:\t"  << static_cast<int >(getInsertPercent()) << std::endl;
    ret << "load_or_run:\t"     << (run == true ? "run":"load") << std::endl;
    ret << "keys_data_path:\t"  << getKeysDataPath() << std::endl;
    ret << "insert_data_path:\t"<< getInsertDataPath() << std::endl;
    ret << "load_data_path:\t"  << getLoadDataPath() << std::endl;
    ret << "compact times:\t"   << static_cast<int >(getCompactTimes()) << std::endl;
    std::string msg;
    ret << "message from " << BenchmarkName << std::endl;
    while (response_message_cq.try_pop(msg)) {
        ret << msg << std::endl;
    }
    return ret.str();
}
std::string BaseSetting::setBaseSetting(std::string &line){

    std::vector<std::string> strvec;
    boost::split(strvec,line,boost::is_any_of("\t"));
    std::string message;
    if (strvec.size() == 0)
        message += "Empty Input\n";

    for(auto& each_kv : strvec){
        //key=value
        size_t div = each_kv.find('=');
        std::string key = each_kv.substr(0,div);
        std::string value = each_kv.substr(div+1);
        if (setFuncMap.count(key) > 0){

            if ((this->*setFuncMap[key])(value)){
                message += "set\t" + key + "\tsuccess\n";
            }
            else{
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
        if (setFuncMap.count(key) == 0) {
            message += "invalid command:" + key + "\n";
            continue;
        }
        if ((this->*setFuncMap[key])(value)){
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

uint8_t BaseSetting::getInsertPercent(void) const {
    return insertPercent.load();
}

bool BaseSetting::strSetInsertPercent(std::string &value) {
    uint8_t val = stoi(value);
    if (val > 100)
        return  false;
    insertPercent.store(val);
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

    std::string ret;
    if (run == false)
        ret = "load";
    else
        ret = "run";
    return ret;
}

bool BaseSetting::strSetLoadOrRun(std::string &value){
    if (value == "run"){
        run = true;
    }else if (value == "load"){
        run = false;
    }else{
        return false;
    }
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

TerarkSetting::TerarkSetting(int argc, char **argv, char *name):Setting(argc,argv,name) {

    funcMap["--compress_ratio"] = std::make_pair(&TerarkSetting::strSetCompressionRatio,
                                                 &TerarkSetting::getCompressionRatio);
}

bool TerarkSetting::strSet(std::string& str){
    //找到‘=’
    auto pos = str.find('=');
    std::string key = str.substr(0,pos);
    std::string value = str.substr(pos+1);
    if (funcMap.count(key) > 0)
        return (this->*(funcMap[key]).first)(value);
    else
        return false;
}
int64_t TerarkSetting::strGet(std::string& key){
    if (funcMap.count(key) > 0)
        return (this->*(funcMap[key].second))();
    else
        return false;
}
uint32_t TerarkSetting::getCompressionRatio() {

}
bool TerarkSetting::strSetCompressionRatio(std::string &val) {
    //找到小数点
}