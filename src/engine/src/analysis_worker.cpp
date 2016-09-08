//
// Created by terark on 16-8-11.
//
#include "stdlib.h"
#include "inttypes.h"
#include "time.h"
#include "cppconn/prepared_statement.h"

#include "analysis_worker.h"
#include "util/system_resource.h"


int TimeBucket::findTimeBucket(uint64_t time) {
    uint64_t t = time / (1000 * 1000 * 1000 * step_in_seconds);
    // printf("find time bucket : %" PRIu64 ", result = %" PRIu64 "\n", time, t*10);
    return t*10;
}
void TimeBucket::upload(int bucket, int ops, int type, bool uploadExtraData){
    if(bucket == 0 || !conn->isValid()) {
        return;
    }
    assert(conn != nullptr);

    // 上传ops数据
    std::unique_ptr<sql::PreparedStatement> ps_ops(conn->prepareStatement("INSERT INTO engine_test_ops_10s(time_bucket, ops, ops_type, engine_name) VALUES(?, ?, ?, ?)"));
    ps_ops->setInt(1, bucket);
    ps_ops->setInt(2, ops);
    ps_ops->setInt(3, type);
    ps_ops->setString(4, engine_name);
    ps_ops->executeUpdate();
    printf("upload time bucket[%d], ops = %d, type = %d\n", bucket, ops, type);

    // 顺便把CPU等数据也上传, 相同时间片只需要上传一次即可
    if(uploadExtraData) {
        // 上传内存数据
        std::vector<int> arr;
        benchmark::getPhysicalMemoryUsage(arr);
        std::unique_ptr<sql::PreparedStatement> ps_memory(conn->prepareStatement("INSERT INTO engine_test_memory_10s(time_bucket, total_memory, free_memory, cached_memory, used_memory, engine_name) VALUES(?, ?, ?, ?, ?, ?)"));
        ps_memory->setInt(1, bucket);
        ps_memory->setInt(2, arr[0]);
        ps_memory->setInt(3, arr[1]);
        ps_memory->setInt(4, arr[2]);
        ps_memory->setInt(5, arr[3]);
        ps_memory->setString(6, engine_name);
        ps_memory->executeUpdate();
        arr.clear();
        printf("total memory = %d\n", arr[0]);

        // 上传CPU数据
        double cpu[2];
        benchmark::getCPUPercentage(cpu);
        std::unique_ptr<sql::PreparedStatement> ps_cpu(conn->prepareStatement("INSERT INTO engine_test_cpu_10s(time_bucket, `usage`, `iowait`, engine_name) VALUES(?, ?, ?, ?)"));
        if(cpu > 0){
            ps_cpu->setInt(1, bucket);
            ps_cpu->setDouble(2, cpu[0]);
            ps_cpu->setDouble(3, cpu[1]);
            ps_cpu->setString(4, engine_name);
            ps_cpu->executeUpdate();
        }
        printf("cpu usage = %f iowait = %f\n", cpu[0], cpu[1]);

        // 上传文件夹尺寸
        int dbsize = benchmark::getDiskUsageByKB(dbpath);
        std::unique_ptr<sql::PreparedStatement> ps_dbsize(conn->prepareStatement("INSERT INTO engine_test_dbsize_10s(time_bucket, `dbsize`, `engine_name`) VALUES(?, ?, ?)"));
        if(dbsize > 0) {
            ps_dbsize->setInt(1, bucket);
            ps_dbsize->setInt(2, dbsize);
            ps_dbsize->setString(3, engine_name);
            ps_dbsize->executeUpdate();
        }
        printf("dbsize = %d KB\n", dbsize);

        // Disk file infomation
        std::string diskinfo;
        std::unique_ptr<sql::PreparedStatement> ps_diskinfo(conn->prepareStatement("INSERT INTO engine_test_diskinfo_10s(time_bucket, `diskinfo`, `engine_name`) VALUES(?, ?, ?)"));
        benchmark::getDiskFileInfo(dbpath, diskinfo);
        if(diskinfo.length() > 0) {
            ps_diskinfo->setInt(1, bucket);
            ps_diskinfo->setString(2, diskinfo);
            ps_diskinfo->setString(3, engine_name);
            ps_diskinfo->executeUpdate();
        }
    }
}

void TimeBucket::add(uint64_t start, uint64_t end, int sampleRate, int type, bool uploadExtraData) {
    if(start <=0 || end <= 0 || start == end) {
        return;
    }
    // when meet the next bucket, upload previous one first, default step is 10 seconds
    int next_bucket = findTimeBucket(start);
    if(next_bucket > current_bucket) {
//            int ops = operation_count * 5 / (int)step_in_seconds; // sample rate is 20%, here we multiply it back.
        int ops = operation_count * 100 / (10 * sampleRate); // sample rate : (0, 100]
        try {
            upload(current_bucket, ops, type, uploadExtraData);
        }catch (std::exception& e){
            printf("%s\n", e.what());
            if(!conn->isValid()){
                try {
                    conn->reconnect();
                    conn->setSchema("benchmark");
                }catch (std::exception& f){
                    printf("%s\n", f.what());
                }
            }
        }
        operation_count = 1;
        current_bucket = next_bucket;
    }else{
        operation_count++;
    }
}


AnalysisWorker::AnalysisWorker(std::string engine_name, Setting* setting) {
    this->engine_name = engine_name;
    this->setting = setting;

    // init mysql connection
    std::unique_ptr<sql::mysql::MySQL_Driver> driver(sql::mysql::get_mysql_driver_instance());
    char* passwd = getenv("MYSQL_PASSWD");
    if(passwd == NULL || strlen(passwd) == 0) {
        printf("no MYSQL_PASSWD set, analysis thread will not upload data!\n");
        //shoud_stop = true;
        return;
    }

    conn = driver->connect("rds432w5u5d17qd62iq3o.mysql.rds.aliyuncs.com:3306", "terark_benchmark", std::string(passwd));
    if(conn != nullptr && conn->isValid()) {
        conn->setSchema("benchmark");
        
        // Delete data from 7 days ago.
        struct timespec t;
        clock_gettime(CLOCK_REALTIME, &t);
        int filter_time = t.tv_sec - 60*60*24*7;
        std::string tables[] = {"engine_test_ops_10s", 
                                "engine_test_memory_10s", 
                                "engine_test_cpu_10s", 
                                "engine_test_dbsize_10s", 
                                "engine_test_diskinfo_10s"
                                };
        for(std::string& table: tables){
            sql::PreparedStatement* pstmt = conn->prepareStatement("DELETE FROM `?` WHERE time_bucket < ?");
            pstmt->setString(1, table);
            pstmt->setInt(2, filter_time);
            pstmt->executeUpdate();
            delete pstmt;
        }
        std::cout<<"database connected!"<<std::endl;
    }else{
        std::cout<<"database connection fault, data will not upload"<<std::endl;
        shoud_stop = true;
    }
}

AnalysisWorker::~AnalysisWorker() {
    delete conn;
}

void AnalysisWorker::stop() {
    shoud_stop = true;
}

void AnalysisWorker::run() {
    std::pair<uint64_t, uint64_t> read_result, insert_result, update_result;
    TimeBucket read_bucket(conn, engine_name, setting->FLAGS_db);
    TimeBucket insert_bucket(conn, engine_name, setting->FLAGS_db);
    TimeBucket update_bucket(conn, engine_name, setting->FLAGS_db);

    while(!shoud_stop) {
        bool b1 = Stats::readTimeDataCq.try_pop(read_result);
        bool b2 = Stats::createTimeDataCq.try_pop(insert_result);
        bool b3 = Stats::updateTimeDataCq.try_pop(update_result);

        bool uploadExtraData = true;
        if(b1){
            read_bucket.add(read_result.first, read_result.second, setting->getSamplingRate(), 1, uploadExtraData);
            uploadExtraData = false;
        }
        if(b2){
            insert_bucket.add(insert_result.first, insert_result.second, setting->getSamplingRate(), 2, uploadExtraData);
            uploadExtraData = false;
        }
        if(b3){
            update_bucket.add(update_result.first, update_result.second, setting->getSamplingRate(), 3, uploadExtraData);
            uploadExtraData = false;
        }
        if(!b1 && !b2 && !b3){
            std::this_thread::sleep_for(std::chrono::milliseconds(5000));
        }
    }
}
