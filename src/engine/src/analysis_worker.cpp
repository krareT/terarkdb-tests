//
// Created by terark on 16-8-11.
//
#include "stdlib.h"
#include "inttypes.h"
#include "analysis_worker.h"
#include "util/system_resource.h"
#include <terark/io/MemStream.hpp>
#include <terark/lcast.hpp>
//#include <terark/util/autoclose.hpp>
#include <mysql.h>
#include <errmsg.h>

using terark::lcast;
using terark::fstring;

class TimeBucket {
private:
    const char* engine_name;
    const std::vector<std::string>& dbdirs;

    int current_bucket = 0;  // seconds
    int operation_count = 0;

public:
    TimeBucket(const char* engineName,
               const std::vector<std::string>& dbdirs)
            : engine_name(engineName),
              dbdirs(dbdirs)
    {}

    void add(terark::AutoGrownMemIO& buf, uint64_t start, uint64_t end, int sampleRate, int type);
};

static int findTimeBucket(uint64_t time) {
    static const uint64_t step_in_seconds = 10; // in seconds
    uint64_t t = time / (1000 * 1000 * 1000 * step_in_seconds);
    // printf("find time bucket : %" PRIu64 ", result = %" PRIu64 "\n", time, t*10);
    return t*10;
}

char* g_passwd = getenv("MYSQL_PASSWD");
static MYSQL g_conn;
static bool  g_hasConn = false;

// also write monitor stat to file
static std::ofstream g_statFile;

static bool Mysql_connect(MYSQL* conn) {
    if(g_passwd == NULL || strlen(g_passwd) == 0) {
        printf("no MYSQL_PASSWD set, analysis thread will not upload data!\n");
        return false;
    }
    printf("Mysql_connect, passwd = %s\n", g_passwd);
    fflush(stdout);
    const char* host = "rds432w5u5d17qd62iq3o.mysql.rds.aliyuncs.com";
    const char* user = "terark_benchmark";
    const char* db = "benchmark";
    if (const char* env = getenv("MYSQL_SERVER")) {
        host = env;
    }
    if (const char* env = getenv("MYSQL_USER")) {
        user = env;
    }
    int port = 3306;
    my_bool myTrue = true;
    mysql_init(conn);
    mysql_options(conn, MYSQL_OPT_RECONNECT, &myTrue); // brain dead reconnect has a fucking bug
    conn->reconnect = true;
    unsigned long clientflag = CLIENT_REMEMBER_OPTIONS;
    if (!mysql_real_connect(conn, host, user, g_passwd, db, port, NULL, clientflag)) {
	//fprintf(stderr, "ERROR: mysql_real_connect failed\n");
	//return false;
        fprintf(stderr
                , "ERROR: mysql_real_connect(host=%s, user=%s, passwd=%s, db=%s, port=%d, NULL, CLIENT_REMEMBER_OPTIONS) = %s\n"
                  "       database connection fault, monitor data will not be uploaded\n"
                , host, user, g_passwd, db, port, mysql_error(conn)
        );
        return false;
    }
    printf("database connected!\n");
    fflush(stdout);
    return true;
}

static MYSQL_STMT* prepare(MYSQL* conn, fstring sql) {
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    int err = mysql_stmt_prepare(stmt, sql.data(), sql.size());
    if (err) {
        fprintf(stderr, "ERROR: mysql_stmt_prepare(%s) = %s\n", sql.c_str(), mysql_error(conn));
        return NULL;
    }
    return stmt;
}
static MYSQL_STMT *ps_ops, *ps_memory, *ps_cpu, *ps_dbsize, *ps_diskinfo;
static void prepair_all_stmt() {
    ps_ops = prepare(&g_conn, "INSERT INTO engine_test_ops_10s(time_bucket, ops, ops_type, engine_name) VALUES(?, ?, ?, ?)");
    ps_memory = prepare(&g_conn, "INSERT INTO engine_test_memory_10s(time_bucket, total_memory, free_memory, cached_memory, used_memory, engine_name) VALUES(?, ?, ?, ?, ?, ?)");
    ps_cpu = prepare(&g_conn, "INSERT INTO engine_test_cpu_10s(time_bucket, `usage`, `iowait`, engine_name) VALUES(?, ?, ?, ?)");
    ps_dbsize = prepare(&g_conn, "INSERT INTO engine_test_dbsize_10s(time_bucket, `dbsize`, `engine_name`) VALUES(?, ?, ?)");
    ps_diskinfo = prepare(&g_conn, "INSERT INTO engine_test_diskinfo_10s(time_bucket, `diskinfo`, `engine_name`) VALUES(?, ?, ?)");
}

void Bind_arg(MYSQL_BIND &b, const int &val) {
    memset(&b, 0, sizeof(b));
    b.buffer_length = 4;
    b.buffer_type = MYSQL_TYPE_LONG;
    b.buffer = (void*)&val;
}
void Bind_arg(MYSQL_BIND& b, const double& val) {
    memset(&b, 0, sizeof(b));
    b.buffer_length = 4;
    b.buffer_type = MYSQL_TYPE_DOUBLE;
    b.buffer = (void*)&val;
}
void Bind_arg(MYSQL_BIND &b, fstring val) {
    memset(&b, 0, sizeof(b));
    b.buffer_length = val.size();
    b.buffer_type = MYSQL_TYPE_VAR_STRING;
    b.buffer = (void*)val.data();
}
template<class... Args>
bool Exec_stmt(st_mysql_stmt* stmt, const Args&... args) {
    if (g_statFile.is_open()) {
        const char* delim = "";
        int a[]{(g_statFile << delim << args, delim = " ")...};
        (void)(a);
        g_statFile << "\n";
        return !g_hasConn;
    }
    else if (!g_hasConn) {
        return false;
    }
    MYSQL_BIND  b[sizeof...(Args)];
    memset(&b, 0, sizeof(b));
    int i = 0;
    std::initializer_list<int>{(Bind_arg(b[i], args), i++)...};
    mysql_stmt_bind_param(stmt, b);
    int err = mysql_stmt_execute(stmt);
    if (err) {
        fprintf(stderr, "WARN: %s = %s\n", BOOST_CURRENT_FUNCTION, stmt->last_error);
        if (CR_SERVER_LOST == err) {
        //    mysql_ping(stmt->mysql); // brain dead mysql
            Mysql_connect(&g_conn);
            prepair_all_stmt();
        }
        return false;
    }
    return true;
}

static int g_prev_sys_stat_bucket = 0;
static void upload_sys_stat(terark::AutoGrownMemIO& buf,
                            const std::vector<std::string>& dbdirs,
                            int bucket, const char* engine_name) {
// 顺便把CPU等数据也上传, 相同时间片只需要上传一次即可
    if (bucket == g_prev_sys_stat_bucket) {
        return;
    }
    g_prev_sys_stat_bucket = bucket;
    int arr[4];
    benchmark::getPhysicalMemoryUsage(arr);
    Exec_stmt(ps_memory, bucket, arr[0], arr[1], arr[2], arr[3], engine_name);
    buf.printf("    total memory = %5.2f GiB", arr[0]/1024.0);

    double cpu[2];
    benchmark::getCPUPercentage(cpu);
    if(cpu > 0){
        Exec_stmt(ps_cpu, bucket, cpu[0], cpu[1], engine_name);
    }
    buf.printf("    cpu usage = %5.2f iowait = %5.2f", cpu[0], cpu[1]);

    int dbsizeKB = benchmark::getDiskUsageByKB(dbdirs);
    if(dbsizeKB > 0) {
        Exec_stmt(ps_dbsize, bucket, dbsizeKB, engine_name);
    }
    buf.printf("    dbsize = %5.2f GiB", dbsizeKB/1024.0/1024);

    std::string diskinfo;
    benchmark::getDiskFileInfo(dbdirs, diskinfo);
    if(diskinfo.length() > 0) {
        Exec_stmt(ps_diskinfo, bucket, fstring(diskinfo), engine_name);
    }
}

void TimeBucket::add(terark::AutoGrownMemIO& buf,uint64_t start, uint64_t end, int sampleRate, int type) {
    // when meet the next bucket, upload previous one first, default step is 10 seconds
    int next_bucket = findTimeBucket(start);
    if(next_bucket > current_bucket) {
        int ops = operation_count * 100 / (10 * sampleRate); // sample rate : (0, 100]
        buf.rewind();
        Exec_stmt(ps_ops, current_bucket, ops, type, engine_name);
        buf.printf("upload statistic time bucket[%d], ops = %7d, type = %d", current_bucket, ops, type);
        upload_sys_stat(buf, dbdirs, current_bucket, engine_name);
        printf("%s\n", buf.begin());
        operation_count = 1;
        current_bucket = next_bucket;
    }else{
        operation_count++;
    }
}

AnalysisWorker::AnalysisWorker(Setting* setting) {
    this->setting = setting;
}

AnalysisWorker::~AnalysisWorker() {
    printf("analysis worker is stopped!\n");
}

void AnalysisWorker::stop() {
    shoud_stop = true;
}

bool g_upload_fake_ops = false;

void AnalysisWorker::run() {
    if (const char* fname = getenv("MONITOR_STAT_FILE")) {
        g_statFile.open(fname);
        if (!g_statFile.is_open()) {
          fprintf(stderr, "open file %s failed = %s\n", strerror(errno));
          return;
        }
    }
    g_hasConn = Mysql_connect(&g_conn);
    if (!g_hasConn) {
        if (!g_statFile.is_open()) {
            shoud_stop = true;
            return;
        }
    }
    else {
  #if 0
      struct timespec t;
      clock_gettime(CLOCK_REALTIME, &t);
      int filter_time = t.tv_sec - 60*60*24*60;
      std::string tables[] = {"engine_test_ops_10s",
                              "engine_test_memory_10s",
                              "engine_test_cpu_10s",
                              "engine_test_dbsize_10s",
                              "engine_test_diskinfo_10s",
      };
      for(std::string& table: tables) {
          std::string sql = "DELETE FROM " + table + " WHERE time_bucket < " + lcast(filter_time);
          mysql_real_query(&g_conn, sql.c_str(), sql.size());
      }
  #endif
      prepair_all_stmt();
    }

    std::pair<uint64_t, uint64_t> read_result, insert_result, update_result;
    TimeBucket read_bucket(engine_name.c_str(), setting->dbdirs);
    TimeBucket insert_bucket(engine_name.c_str(), setting->dbdirs);
    TimeBucket update_bucket(engine_name.c_str(), setting->dbdirs);
    terark::AutoGrownMemIO buf;
    shoud_stop = false;
    while(!shoud_stop) {
        bool b1 = Stats::readTimeDataCq.try_pop(read_result);
        bool b2 = Stats::createTimeDataCq.try_pop(insert_result);
        bool b3 = Stats::updateTimeDataCq.try_pop(update_result);
        if (b1) {
            read_bucket.add(buf, read_result.first, read_result.second, setting->getSamplingRate(), 1);
        }
        if (b2) {
            insert_bucket.add(buf, insert_result.first, insert_result.second, setting->getSamplingRate(), 2);
        }
        if (b3) {
            update_bucket.add(buf, update_result.first, update_result.second, setting->getSamplingRate(), 3);
        }
        if (!b1 && !b2 && !b3) {
            timespec ts1;
            clock_gettime(CLOCK_REALTIME, &ts1);
            unsigned long long tt = 1000000000ull * ts1.tv_sec + ts1.tv_nsec;
            int curr_bucket = findTimeBucket(tt);
            if (curr_bucket > g_prev_sys_stat_bucket) {
                buf.rewind();
                if (g_upload_fake_ops) {
                    int ops = 0, op_type = 2;
                    Exec_stmt(ps_ops, curr_bucket, ops, op_type, engine_name.c_str());
                    buf.printf("upload statistic time bucket[%d], OPS = %7d, type = %d", curr_bucket, ops, op_type);
                }
                else {
                    buf.printf("upload statistic time bucket[%d], nop", curr_bucket);
                }
                upload_sys_stat(buf, setting->dbdirs, curr_bucket, engine_name.c_str());
                printf("%s\n", buf.begin());
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5000));
        }
    }
    if (g_hasConn) {
        mysql_stmt_close(ps_ops);
        mysql_stmt_close(ps_memory);
        mysql_stmt_close(ps_cpu);
        mysql_stmt_close(ps_dbsize);
        mysql_stmt_close(ps_diskinfo);
        mysql_close(&g_conn);
    }
}
