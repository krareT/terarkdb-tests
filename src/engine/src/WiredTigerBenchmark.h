//
// Created by terark on 8/24/16.
//

#ifndef TERARKDB_TEST_FRAMEWORK_WIREDTIGERBENCHMARK_H
#define TERARKDB_TEST_FRAMEWORK_WIREDTIGERBENCHMARK_H

#include "Benchmark.h"

using namespace leveldb;
class WiredTigerBenchmark : public Benchmark{
private:
    WT_CONNECTION *conn_;
    std::string uri_;
    int db_num_;
    int num_;
    int sync_;
    void PrintHeader() {
        const int kKeySize = 16;
        PrintEnvironment();
        fprintf(stdout, "Keys:       %d bytes each\n", kKeySize);
        fprintf(stdout, "Values:     %d bytes each (%d bytes after compression)\n",
                setting.FLAGS_value_size,
                static_cast<int>(setting.FLAGS_value_size * setting.FLAGS_compression_ratio + 0.5));
        fprintf(stdout, "Entries:    %d\n", num_);
        fprintf(stdout, "RawSize:    %.1f MB (estimated)\n",
                ((static_cast<int64_t>(kKeySize + setting.FLAGS_value_size) * num_)
                 / 1048576.0));
        fprintf(stdout, "FileSize:   %.1f MB (estimated)\n",
                (((kKeySize + setting.FLAGS_value_size * setting.FLAGS_compression_ratio) * num_)
                 / 1048576.0));
        PrintWarnings();
        fprintf(stdout, "------------------------------------------------\n");
    }
    void PrintWarnings() {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
        fprintf(stdout,
                "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n"
        );
#endif
#ifndef NDEBUG
        fprintf(stdout,
                "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif

        // See if snappy is working by attempting to compress a compressible string
        const char text[] = "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";
        std::string compressed;
        if (!port::Snappy_Compress(text, sizeof(text), &compressed)) {
            fprintf(stdout, "WARNING: Snappy compression is not enabled\n");
        } else if (compressed.size() >= sizeof(text)) {
            fprintf(stdout, "WARNING: Snappy compression is not effective\n");
        }
    }
    void PrintEnvironment() {
        int wtmaj, wtmin, wtpatch;
        const char *wtver = wiredtiger_version(&wtmaj, &wtmin, &wtpatch);
        fprintf(stdout, "WiredTiger:    version %s, lib ver %d, lib rev %d patch %d\n",
                wtver, wtmaj, wtmin, wtpatch);
        fprintf(stderr, "WiredTiger:    version %s, lib ver %d, lib rev %d patch %d\n",
                wtver, wtmaj, wtmin, wtpatch);

#if defined(__linux)
        time_t now = time(NULL);
        fprintf(stderr, "Date:       %s", ctime(&now));  // ctime() adds newline

        FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
        if (cpuinfo != NULL) {
            char line[1000];
            int num_cpus = 0;
            std::string cpu_type;
            std::string cache_size;
            while (fgets(line, sizeof(line), cpuinfo) != NULL) {
                const char* sep = strchr(line, ':');
                if (sep == NULL) {
                    continue;
                }
                Slice key = TrimSpace(Slice(line, sep - 1 - line));
                Slice val = TrimSpace(Slice(sep + 1));
                if (key == "model name") {
                    ++num_cpus;
                    cpu_type = val.ToString();
                } else if (key == "cache size") {
                    cache_size = val.ToString();
                }
            }
            fclose(cpuinfo);
            fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
            fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
        }
#endif
    }
public:
    WiredTigerBenchmark(Setting &setting1) : Benchmark(setting1) {

    }
    ~WiredTigerBenchmark() {
    }
private:
    size_t getKeyAndValue(std::string &str,std::string &key,std::string &val){
        std::vector<std::string> strvec;
        boost::split(strvec,str,boost::is_any_of("\t"));
        key.clear();
        val.clear();
        assert(key.size() + val.size() == 0);
        if ( strvec.size() < 8)
            return 0;
        key = strvec[2] + strvec[7];
        for(int i = 0; i < strvec.size(); i ++){
            if (i == 2 || i == 7)
                continue;
            val.append(strvec[i]);
        }
        val = str;
        return strvec.size();
    }
    ThreadState* newThreadState(std::atomic<std::vector<uint8_t >*>* whichEPlan,
    std::atomic<std::vector<uint8_t >*>* whichSPlan){
        return new ThreadState(threads.size(),conn_,whichEPlan,whichSPlan);
    }
    void Load(void){
        DoWrite(true);
    }
    void Close(void){
        conn_->close(conn_, NULL);
        conn_ = NULL;
    }
    bool ReadOneKey(ThreadState *thread) {
        std::string str;
        if ( getRandomKey(str,thread->randGenerator) == false){
            return false;
        }
        WT_CURSOR *cursor;

        int ret = thread->session->open_cursor(thread->session, uri_.c_str(), NULL, NULL, &cursor);
        if (ret != 0) {
            fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
            exit(1);
        }
        int found = 0;
        std::string key(str);
        cursor->set_key(cursor, key.c_str());
        if (cursor->search(cursor) == 0) {
            found++;
            const char* val;
            ret = cursor->get_value(cursor, &val);
            assert(ret == 0);
        }
        cursor->close(cursor);
        return found > 0;
    }

    bool UpdateOneKey(ThreadState *thread) {

        std::string str;
        if ( getRandomKey(str,thread->randGenerator) == false){
            return false;
        }
        WT_CURSOR *cursor;
        int ret = thread->session->open_cursor(thread->session, uri_.c_str(), NULL,NULL, &cursor);
        if (ret != 0){
            fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
            return false;
        }
        std::string key;
        std::string val;
        getKeyAndValue(str,key,val);
        cursor->set_key(cursor, key.c_str());
        if (cursor->search(cursor) != 0){
            std::cerr << "cursor search error :" << key << std::endl;
            cursor->close(cursor);
            return false;
        }
        const char *value;
        ret = cursor->get_value(cursor,&val);
        assert(ret == 0);
        cursor->set_value(cursor,val);
        ret = cursor->insert(cursor);
        if (ret != 0){
            std::cerr << "cursor insert error :" << key << std::endl;
            cursor->close(cursor);
            return false;
        }
        cursor->close(cursor);
        return true;
    }
    bool InsertOneKey(ThreadState *thread){
        WT_CURSOR *cursor;
        int ret = thread->session->open_cursor(thread->session, uri_.c_str(), NULL,NULL, &cursor);
        if (ret != 0) {
            fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
            exit(1);
        }
        std::string str;
        if ( !updateDataCq.try_pop(str)) {
            cursor->close(cursor);
            return false;
        }
        std::string key;
        std::string val;

        ret = getKeyAndValue(str,key,val);
        if (ret == 0) {
            cursor->close(cursor);
            return false;
        }
        cursor->set_key(cursor, key.c_str());
        cursor->set_value(cursor,val.c_str());

        ret = cursor->insert(cursor);
        if (ret != 0) {
            fprintf(stderr, "set error: %s\n", wiredtiger_strerror(ret));
            cursor->close(cursor);
            return false;
        }
        cursor->close(cursor);
        pushKey(key);
        return true;
    }
    void Open() {
        PrintHeader();
        PrintEnvironment();
        PrintWarnings();
#define SMALL_CACHE 10*1024*1024
        std::stringstream config;
        config.str("");
        if (!setting.FLAGS_use_existing_db) {
            config << "create";
        }
        if (setting.FLAGS_cache_size > 0)
            config << ",cache_size=" << setting.FLAGS_cache_size;
        config << ",log=(enabled,recover=on)";
        config << ",checkpoint=(log_size=64MB,wait=60)";
        /* TODO: Translate write_buffer_size - maybe it's chunk size?
           options.write_buffer_size = FLAGS_write_buffer_size;
           */
#ifndef SYMAS_CONFIG
        config << ",extensions=[libwiredtiger_snappy.so]";
#endif
        //config << ",verbose=[lsm]";
        Env::Default()->CreateDir(setting.FLAGS_db);
        printf("WT config : %s\n",config.str().c_str());
        wiredtiger_open(setting.FLAGS_db, NULL, config.str().c_str(), &conn_);
        assert(conn_ != NULL);

        WT_SESSION *session;
        conn_->open_session(conn_, NULL, NULL, &session);
        assert(session != NULL);

        char uri[100];
        snprintf(uri, sizeof(uri), "%s:dbbench_wt-%d",
                 setting.FLAGS_use_lsm ? "lsm" : "table", db_num_);
        uri_ = uri;

        if (!setting.FLAGS_use_existing_db) {
            // Create tuning options and create the data file
            config.str("");
            config << "key_format=S,value_format=S";
            config << ",columns=[p,val]";
            config << ",prefix_compression=true";
            config << ",checksum=off";

            if (setting.FLAGS_cache_size < SMALL_CACHE && setting.FLAGS_cache_size > 0) {
                config << ",internal_page_max=4kb";
                config << ",leaf_page_max=4kb";
                config << ",memory_page_max=" << setting.FLAGS_cache_size;
            } else {
                config << ",internal_page_max=16kb";
                config << ",leaf_page_max=16kb";
                if (setting.FLAGS_cache_size > 0) {
                    long memmax = setting.FLAGS_cache_size * 0.75;
                    // int memmax = setting.FLAGS_cache_size * 0.75;
                    config << ",memory_page_max=" << memmax;
                }
            }
            if (setting.FLAGS_use_lsm) {
                config << ",lsm=(";
                if (setting.FLAGS_cache_size > SMALL_CACHE)
                    config << ",chunk_size=" << setting.FLAGS_write_buffer_size;
                if (setting.FLAGS_bloom_bits > 0)
                    config << ",bloom_bit_count=" << setting.FLAGS_bloom_bits;
                else if (setting.FLAGS_bloom_bits == 0)
                    config << ",bloom=false";
                config << ")";
            }
#ifndef SYMAS_CONFIG
            config << ",block_compressor=snappy";
#endif
            fprintf(stderr, "Creating %s with config %s\n",uri_.c_str(), config.str().c_str());
            int ret = session->create(session, uri_.c_str(), config.str().c_str());
            if (ret != 0) {
                fprintf(stderr, "create error: %s\n", wiredtiger_strerror(ret));
                exit(1);
            }

            session->close(session, NULL);
        }
    }
    void DoWrite(bool seq) {
        std::cout << "DoWrite!" << std::endl;
        std::stringstream txn_config;
        txn_config.str("");
        txn_config << "isolation=snapshot";
        if (sync_)
            txn_config << ",sync=full";
        else
            txn_config << ",sync=none";
        WT_CURSOR *cursor;
        std::stringstream cur_config;
        cur_config.str("");
        cur_config << "overwrite";
        WT_SESSION *session;
        conn_->open_session(conn_, NULL, NULL, &session);
        int ret = session->open_cursor(session, uri_.c_str(), NULL, cur_config.str().c_str(), &cursor);
        if (ret != 0) {
            fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
            exit(1);
        }

        std::string str;
        long long recordnumber = 0;
        FILE *file = fopen(setting.getLoadDataPath().c_str(),"r");
        posix_fadvise(fileno(file),0,0,POSIX_FADV_SEQUENTIAL);
//        LineBuf line;
        std::string key;
        std::string val;
        char *buf = NULL;
        size_t n = 0;
        while( -1 != getline(&buf,&n,file)) {
            str = buf;
            if ( n > 1024 * 1024){
                free(buf);
                buf = NULL;
                n = 0;
            }
            ret = getKeyAndValue(str,key,val);
            if (ret == 0)
                continue;

            cursor->set_key(cursor, key.c_str());
            cursor->set_value(cursor,val.c_str());
            //std::cout << "key:" << key << std::endl;
            int ret = cursor->insert(cursor);
            if (ret != 0) {
                fprintf(stderr, "set error: %s\n", wiredtiger_strerror(ret));
                exit(1);
            }
            pushKey(key);
            recordnumber++;
            if (recordnumber % 100000 == 0)
                std::cout << "Record number: " << recordnumber << std::endl;

        }
        if ( buf != NULL){
            free(buf);
            buf = NULL;
        }
        cursor->close(cursor);
        time_t now;
        struct tm *timenow;
        time(&now);
        timenow = localtime(&now);
        printf("recordnumber %lld,  time %s\n",recordnumber, asctime(timenow));
    }
};


#endif //TERARKDB_TEST_FRAMEWORK_WIREDTIGERBENCHMARK_H
