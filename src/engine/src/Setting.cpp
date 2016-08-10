//
// Created by terark on 16-8-1.
//

#include <include/leveldb/env.h>
#include "Setting.h"
#include <iostream>
#include <include/leveldb/options.h>

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
            std::cout << "cache_size " << size << std::endl;
            FLAGS_cache_size = size;
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

    shuff = (int *)malloc(FLAGS_threads * sizeof(int));
    for (int i=0; i<FLAGS_threads; i++)
        shuff[i] = i;

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
        } else{
            fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
            exit(1);
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

    if (FLAGS_resource_data == NULL) {
        fprintf(stderr, "Please input the resource data file\n");
        exit(-1);
    }

    shuff = (int *)malloc(FLAGS_threads * sizeof(int));
    for (int i=0; i<FLAGS_threads; i++)
        shuff[i] = i;


}

Setting::Setting(int argc,char **argv,char *name){
    if ( strcmp(name,"WiredTiger") == 0){
        wiredTigerSetting(argc,argv);
    }else if (strcmp(name,"Terark") == 0){
        terarkSetting(argc,argv);
    }else if (strcmp(name,"compact") == 0){
        terarkSetting(argc,argv);
    }else{
        fprintf(stderr,"error:argv[1]:%s",argv[1]);
        exit(1);
    }
    baseSetting.setThreadNums(FLAGS_threads);

}
BaseSetting::BaseSetting(){

    READ_PERCENT.store(100);
    STOP.store(false);
    setFunc_map["stop"] = &BaseSetting::strSetStop;
    setFunc_map["read_percent"] = &BaseSetting::strSetReadPercent;
    setFunc_map["thread_num"] = &BaseSetting::strSetThreadNums;
    setFunc_map["time_data_capcity"] = &BaseSetting::strSetTimeDataCapicity;
}
bool BaseSetting::strSetTimeDataCapicity(std::string& value){

    uint64_t val = stoi(value);
    if ( val > TIME_DATA_MAX_CAPCITY)
        return false;
    data_capcity.store(val);
    return true;
}
bool BaseSetting::strSetStop(std::string &value) {

    if (value == "true"){
        STOP.store(true);
        return true;
    }else if (value == "false"){
        STOP.store(false);
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
bool BaseSetting::ifStop()
{
    return STOP.load();
}
void BaseSetting::setReadPercent(uint8_t readPercent) {

    if ( readPercent < 0)
        readPercent = 0;
    if ( readPercent > 100)
        readPercent = 100;
    READ_PERCENT.store(readPercent);
}

uint8_t BaseSetting::getReadPercent(void) {

    return READ_PERCENT.load();
}
void BaseSetting::setThreadNums(uint32_t num)
{
    THREAD_NUMS.store(num);
}
uint32_t BaseSetting::getThreadNums(void) {
    return THREAD_NUMS.load();
}
void BaseSetting::setStop(void){
    STOP.store(true);
}
std::string BaseSetting::toString() {

    char buf[1024];
    snprintf(buf,1024,"read_percent:%u\nstop:%u\nthreads_num:%u\n",
             READ_PERCENT.load(),STOP.load(),THREAD_NUMS.load());
    std::string msg(buf);
    return msg;
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
        if (setFunc_map.count(key) > 0){

            if ((this->*setFunc_map[key])(value)){
                message += "set\t" + key + "\tsuccess\t";
            }
            else{
                message += "set\t" + key + "\tfailure\t";
            }
        }else{
            message += "invalid command:" + key + "\t";
        }
    }
    return message + toString();
}