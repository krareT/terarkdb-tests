//
// Created by terark on 16-8-11.
//
#include "inttypes.h"

#include "cppconn/prepared_statement.h"

#include "analysis_worker.h"
#include "Stats.h"

class TimeBucket{
private:
    sql::Connection* conn = nullptr;

    int findTimeBucket(uint64_t time, int step_in_seconds) {
        uint64_t t = time / (step_in_seconds * 1000 * 1000);
        return (int)t;
    }

    /**
     * upload data into MySQL cloud database
     * @param bucket
     * @param ops
     * @param type
     */
    void upload(int bucket, int ops, int type){
        assert(conn != nullptr);
        sql::PreparedStatement* ps = conn->prepareStatement("INSERT INTO engine_test_10s(time_slice, ops, ops_type) VALUES(?, ?, ?)");
        ps->setInt(1, bucket);
        ps->setInt(2, ops);
        ps->setInt(3, type);
        ps->executeUpdate();
        delete ps;
        printf("%d - %d - %d\n", bucket, ops, type);
    }

public:
    uint64_t current_bucket = 0;  // seconds
    int operation_count = 0;

    TimeBucket(sql::Connection* const conn){
        this->conn = conn;
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
        int next_bucket = findTimeBucket(start, 10);
        if(next_bucket > current_bucket) {
            int ops = operation_count / 10;
            upload(current_bucket, ops, type);
            operation_count = 1;
            current_bucket = next_bucket;
        }else{
            operation_count++;
        }
    }
};


AnalysisWorker::AnalysisWorker() {
    // init mysql connection
    sql::mysql::MySQL_Driver* driver = sql::mysql::get_mysql_driver_instance();
    conn = driver->connect("rds432w5u5d17qd62iq3o.mysql.rds.aliyuncs.com:3306", "terark_benchmark", "benchmark@123");

    if(conn != nullptr && conn->isValid()) {
        conn->setSchema("benchmark");
        sql::PreparedStatement* pstmt = conn->prepareStatement("UPDATE engine_test_10s set `time_slice` = 1 WHERE 1 = 0");
        pstmt->executeUpdate();
        std::cout<<"database connected!"<<std::endl;
        delete pstmt;
    }else{
        std::cout<<"database failed"<<std::endl;
    }
}

AnalysisWorker::~AnalysisWorker() {
    delete conn;
}

void AnalysisWorker::run() {
    printf("Analysis worker is running ... \n");
    std::pair<uint64_t, uint64_t> read_result, insert_result, update_result;
    assert(conn != NULL);
    TimeBucket read_bucket(conn);
    TimeBucket insert_bucket(conn);
    TimeBucket update_bucket(conn);

    while(true) {
        bool b1 = Stats::readTimeDataCq.try_pop(read_result);
        bool b2 = Stats::createTimeDataCq.try_pop(insert_result);
        bool b3 = Stats::updateTimeDataCq.try_pop(update_result);

        if(b1){
            read_bucket.add(read_result.first, read_result.second, 0);
        } else if(b2){
            insert_bucket.add(insert_result.first, insert_result.second, 1);
        } else if(b3){
            update_bucket.add(update_result.first, update_result.second, 2);
        } else {
            printf("Analysis worker sleep for 3 seconds\n");
            std::this_thread::sleep_for(std::chrono::milliseconds(3000));
        }
        // TODO break the loop when receive ctrl+C
    }
}