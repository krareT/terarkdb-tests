//
// Created by terark on 16-8-11.
//
#include "stdlib.h"
#include "inttypes.h"
#include "cppconn/prepared_statement.h"

#include "analysis_worker.h"
#include "util/system_resource.h"

class TimeBucket{
private:
    sql::Connection* conn = nullptr;
    uint64_t step_in_seconds = 10; // in seconds
    std::string engine_name;

    int findTimeBucket(uint64_t time) {
        uint64_t t = time / (1000 * 1000 * 1000 * step_in_seconds);
        // printf("find time bucket : %" PRIu64 ", result = %" PRIu64 "\n", time, t*10);
        return t*10;
    }

    /**
     * upload data into MySQL cloud database
     * @param bucket
     * @param ops
     * @param type
     */
    void upload(int bucket, int ops, int type){
        if(bucket == 0) {
            return;
        }
        assert(conn != nullptr);

        // 上传ops数据
        sql::PreparedStatement* ps_ops = conn->prepareStatement("INSERT INTO engine_test_ops_10s(time_bucket, ops, ops_type, engine_name) VALUES(?, ?, ?, ?)");
        ps_ops->setInt(1, bucket);
        ps_ops->setInt(2, ops);
        ps_ops->setInt(3, type);
        ps_ops->setString(4, engine_name);
        ps_ops->executeUpdate();
        delete ps_ops;
        printf("upload time bucket[%d], ops = %d, type = %d\n", bucket, ops, type);

        // 当上传read数据的时候,顺便把CPU等数据也上传
        if(type == 1) {
            // 上传内存数据
            sql::PreparedStatement* ps_memory = conn->prepareStatement("INSERT INTO engine_test_memory_10s(time_bucket, total_memory, free_memory, cached_memory, used_memory, engine_name) VALUES(?, ?, ?, ?, ?, ?)");
            std::vector<int> arr = benchmark::getPhysicalMemoryUsage();
            ps_memory->setInt(1, bucket);
            ps_memory->setInt(2, arr[0]);
            ps_memory->setInt(3, arr[1]);
            ps_memory->setInt(4, arr[2]);
            ps_memory->setInt(5, arr[3]);
            ps_memory->setString(6, engine_name);
            ps_memory->executeUpdate();
            arr.clear();
            delete ps_memory;
            printf("total memory = %d\n", arr[0]);

            // 上传CPU数据
            sql::PreparedStatement* ps_cpu = conn->prepareStatement("INSERT INTO engine_test_cpu_10s(time_bucket, `usage`, engine_name) VALUES(?, ?, ?)");
            double cpu = benchmark::getCPUPercentage();
            if(cpu > 0){
                ps_cpu->setInt(1, bucket);
                ps_cpu->setDouble(2, cpu);
                ps_cpu->setString(3, engine_name);
                ps_cpu->executeUpdate();
            }
            delete ps_cpu;
            printf("cpu usage = %f\n", cpu);

        }
    }

public:
    uint64_t current_bucket = 0;  // seconds
    int operation_count = 0;

    TimeBucket(sql::Connection* const conn, std::string engine_name){
        this->conn = conn;
        this->engine_name = engine_name;
    }

    /**
     *
     * @param start
     * @param end
     * @param type 0:read, 1:insert, 2:update
     */
    void add(uint64_t start, uint64_t end, int type){
        if(start <=0 || end <= 0 || start == end) {
            return;
        }
        // when meet the next bucket, upload previous one first, default step is 10 seconds
        int next_bucket = findTimeBucket(start);
        if(next_bucket > current_bucket) {
//            int ops = operation_count * 5 / (int)step_in_seconds; // sample rate is 20%, here we multiply it back.
            int ops = operation_count * 5 / 10; // multiple 5 for sample rate.
            try {
                upload(current_bucket, ops, type);
            }catch (std::exception& e){
                printf("%s\n", e.what());
            }
            operation_count = 1;
            current_bucket = next_bucket;
        }else{
            operation_count++;
        }
    }
};


AnalysisWorker::AnalysisWorker(std::string engine_name) {
    this->engine_name = engine_name;
    // init mysql connection
    sql::mysql::MySQL_Driver* driver = sql::mysql::get_mysql_driver_instance();
    const char* passwd = getenv("MYSQL_PASSWD");
    if(passwd == NULL || strlen(passwd) == 0) {
        printf("no MYSQL_PASSWD set, exit analysis thread!\n");
        shoud_stop = true;
        return;
    }
    conn = driver->connect("rds432w5u5d17qd62iq3o.mysql.rds.aliyuncs.com:3306", "terark_benchmark", std::string(passwd));
    if(conn != nullptr && conn->isValid()) {
        conn->setSchema("benchmark");
        sql::PreparedStatement* pstmt = conn->prepareStatement("DELETE FROM engine_test_ops_10s WHERE time_bucket = 0");
        pstmt->executeUpdate();
        std::cout<<"database connected!"<<std::endl;
        delete pstmt;
    }else{
        std::cout<<"database connection fault"<<std::endl;
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
    TimeBucket read_bucket(conn, engine_name);
    TimeBucket insert_bucket(conn, engine_name);
    TimeBucket update_bucket(conn, engine_name);

    while(!shoud_stop) {
        bool b1 = Stats::readTimeDataCq.try_pop(read_result);
        bool b2 = Stats::createTimeDataCq.try_pop(insert_result);
        bool b3 = Stats::updateTimeDataCq.try_pop(update_result);

        if(b1){
            read_bucket.add(read_result.first, read_result.second, 1);
        }
        if(b2){
            insert_bucket.add(insert_result.first, insert_result.second, 2);
        }
        if(b3){
            update_bucket.add(update_result.first, update_result.second, 3);
        }
        if(!b1 && !b2 && !b3){
            std::this_thread::sleep_for(std::chrono::milliseconds(5000));
        }
    }
}