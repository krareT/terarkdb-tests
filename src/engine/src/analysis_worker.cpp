//
// Created by terark on 16-8-11.
//
#include "inttypes.h"
#include "analysis_worker.h"
#include "Stats.h"

class TimeBucket{
private:
    MySQL* conn = nullptr;

    int findTimeBucket(uint64_t time, int step_in_seconds) {
        return time / (step_in_seconds * 1000 * 1000);
    }

    /**
     * upload data into MySQL cloud database
     * @param bucket
     * @param ops
     * @param type
     */
    void upload(int bucket, int ops, int type){
        // upload data into aliyun rds
    }

public:
    uint64_t current_bucket = 0;  // seconds
    int operation_count = 0;

    TimeBucket(MySQL* const conn){
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
    const char user[] = "root";
    const char passwd[] = "";
    const char host[] = "";
    const char table_10s_log = "";
    int port = 3306;
    if(mysql_real_connect(&conn, host, user, passwd, table_10s_log, port, NULL, 0)) {
        std::cout<<"database connected!"<<std::endl;
    }else{
        std::cout<<"database failed"<<std::endl;
    }
}

void AnalysisWorker::run() {
    printf("Analysis worker is running ... \n");
    std::pair<uint64_t, uint64_t> read_result, insert_result, update_result;
    TimeBucket read_bucket(&conn);
    TimeBucket insert_bucket(&conn);
    TimeBucket update_bucket(&conn);

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
    }
}