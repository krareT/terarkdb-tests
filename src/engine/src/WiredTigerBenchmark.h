//
// Created by terark on 16-8-2.
//

#ifndef TERARKDB_TEST_FRAMEWORK_WIREDTIGERBENCHMARK_H
#define TERARKDB_TEST_FRAMEWORK_WIREDTIGERBENCHMARK_H

#include <wiredtiger.h>
#include "src/leveldb.h"
#include <sys/types.h>
#include <sys/time.h>
#include <stdlib.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <sstream>
#include <vector>

#include <iostream>
#include <fstream>
#include <string.h>

#include <unistd.h>
#include <errno.h> /* For ETIMEDOUT */
#include "util/crc32c.h"
#include "util/histogram.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/testutil.h"
#include "port/port.h"
#include "wiredtiger.h"
#include "TcpServer.h"

#include <terark/util/autofree.hpp>
#include <terark/util/fstrvec.hpp>
#include <terark/fstring.hpp>
#include <thread>

namespace leveldb {



// Helper for quickly generating random data.
        class RandomGenerator {
        private:
            std::string data_;
            int pos_;
            Setting &setting;
        public:
            RandomGenerator(Setting &setting1):setting(setting1) {
                // We use a limited amount of data over and over again and ensure
                // that it is larger than the compression window (32KB), and also
                // large enough to serve all typical value sizes we want to write.
                Random rnd(301);
                std::string piece;
                while (data_.size() < 1048576) {
                    // Add a short fragment that is as compressible as specified
                    // by setting.FLAGS_compression_ratio.

                    test::CompressibleString(&rnd, setting.FLAGS_compression_ratio, 100, &piece);
                    data_.append(piece);
                }
                pos_ = 0;
            }

            Slice Generate(int len) {
                if (pos_ + len > data_.size()) {
                    pos_ = 0;
                    assert(len < data_.size());
                }
                pos_ += len;
                return Slice(data_.data() + pos_ - len, len);
            }
        };


    class WiredTigerBenchmark {
    private:
        WT_CONNECTION *conn_;
        std::string uri_;
        std::string urii_;
        int db_num_;
        int num_;
        int value_size_;
        int entries_per_batch_;
        int sync_;
        int reads_;
        int heap_counter_;

        terark::fstrvec allkeys_;
        Setting &setting;
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
        WiredTigerBenchmark
                (Setting &setting1)
                : num_(setting1.FLAGS_num),
                  value_size_(setting1.FLAGS_value_size),
                  db_num_(0),
                  entries_per_batch_(1),
                  reads_(setting1.FLAGS_reads < 0 ? setting1.FLAGS_num : setting1.FLAGS_reads),
                  heap_counter_(0),
                    setting(setting1) {
            std::vector<std::string> files;
            Env::Default()->GetChildren(setting.FLAGS_db, &files);
            for (int i = 0; i < files.size(); i++) {
                if (Slice(files[i]).starts_with("heap-")) {
                    Env::Default()->DeleteFile(std::string(setting.FLAGS_db) + "/" + files[i]);
                }
            }
            if (!setting.FLAGS_use_existing_db) {
                for (int i = 0; i < files.size(); i++) {
                    std::string file_name(setting.FLAGS_db);
                    file_name += "/";
                    file_name += files[i];
                    Env::Default()->DeleteFile(file_name.c_str());
                }
            }
        }

        ~WiredTigerBenchmark() {
        }

        void Run() {
            PrintHeader();
            std::cout << " Run() " << std::endl;

            std::ifstream ifs(setting.FLAGS_resource_data);
            std::string str;
            std::string key1;
            std::string key2;

            while(getline(ifs, str)) {
                if (str.find("product/productId:") == 0) {
                    key1 = str.substr(19);
                    continue;
                }
                if (str.find("review/userId:") == 0) {
                    key2 = str.substr(15);
                    allkeys_.push_back(key1 + " " + key2);
                    continue;
                }
            }
            ifs.close();
            allkeys_.shrink_to_fit();
            printf("allkeys_.mem_size=%zd\n", allkeys_.full_mem_size());
            assert(allkeys_.size() == setting.FLAGS_num);

            Open();
            DoWrite(true);
            void (WiredTigerBenchmark::*method)(ThreadState *);


            struct timespec start, end;
            clock_gettime(CLOCK_MONOTONIC, &start);

            method = &WiredTigerBenchmark::ReadWhileWriting;

            RunWiredTigerBenchmark(setting.baseSetting.getThreadNums(), "RWThread", method);

            clock_gettime(CLOCK_MONOTONIC, &end);
            long long timeuse = 1000000000 * ( end.tv_sec - start.tv_sec ) + end.tv_nsec -start.tv_nsec;
            printf("RunWiredTigerBenchmark total time is : %lld \n", timeuse/1000000000);

            allkeys_.erase_all();
            std::cout << "---------" << std::endl;
            if (conn_ != NULL) {
                conn_->close(conn_, NULL);
                conn_ = NULL;
            }
        }

    private:
        struct ThreadArg {
            WiredTigerBenchmark* bm;
            ThreadState* thread;
            void (WiredTigerBenchmark::*method)(ThreadState*);

            ThreadArg(WiredTigerBenchmark* bm0,ThreadState* ts,
                      void (WiredTigerBenchmark::*m)(ThreadState*))
                    :   bm(bm0),
                        thread(ts),
                        method(m)
            {}
        };
        static void ThreadBody(void *v) {
            ThreadArg *arg = reinterpret_cast<ThreadArg *>(v);
            leveldb::ThreadState *thread = arg->thread;

            thread->stats->Start();
            (arg->bm->*(arg->method))(thread);
            thread->stats->Stop();
        }
        void adjustThreadNum(   std::vector<std::pair<std::thread,ThreadArg*>> &threads,
                                uint32_t target,
                                void (WiredTigerBenchmark::*method)(leveldb::ThreadState *),
                                std::atomic<std::vector<uint8_t >*>* which){

            while (target > threads.size()){
                //Add new thread.
                ThreadArg *threadArg = new ThreadArg(this,new leveldb::ThreadState(threads.size(),setting,conn_,which),method);
                threads.push_back( std::make_pair(std::thread(ThreadBody,threadArg),threadArg));
            }
            while (target < threads.size()){
                //delete thread
                threads.back().second->thread->STOP.store(true);
                threads.back().first.join();
                delete threads.back().second->thread;
                delete threads.back().second;
                threads.pop_back();
            }
        }
        void gatherThreadInfo(std::vector<std::pair<std::thread,ThreadArg*>> &threads){

            std::vector<uint64_t > readVec;
            std::vector<uint64_t > writeVec;
            uint64_t total_read = 0;
            uint64_t total_write = 0;
            std::cout << "------------------" << std::endl;

            for(auto& eachThreadInfo : threads){
                readVec.push_back(eachThreadInfo.second->thread->stats->typedDone_[1].exchange(0));
                writeVec.push_back(eachThreadInfo.second->thread->stats->typedDone_[0].exchange(0));
                total_read += readVec.back();
                total_write += writeVec.back();
            }
            for(int i = 0; i < readVec.size();i++){
                std::cout << "Thread " << i << " read " << readVec[i] << " write " << writeVec[i] << std::endl;
            }
            std::cout << "Total read " << total_read << " write " << total_write << std::endl;


        }
        void RunWiredTigerBenchmark(int n, leveldb::Slice name,
                                void (WiredTigerBenchmark::*method)(leveldb::ThreadState *)) {
            std::cout << "Run WiredTiger Benchmark!" << std::endl;
            leveldb::SharedState shared;
            shared.total = n;
            shared.num_initialized = 0;
            shared.num_done = 0;
            shared.start = false;
            shared.setting = & (this->setting);

            std::vector<std::pair<std::thread,ThreadArg*>> threads;
            int old_readPercent = -1;

            std::atomic<std::vector<uint8_t > *> planAddr;
            std::vector<uint8_t > plan[2];
            bool backupPlan = false;//不作真假，只用来切换plan
            leveldb::Stats::timeInit();
            while( !setting.baseSetting.ifStop()){

                int readPercent = setting.baseSetting.getReadPercent();
                if (readPercent != old_readPercent){
                    old_readPercent = readPercent;
                    updatePlan(plan[backupPlan],readPercent);
                    planAddr.store( &(plan[backupPlan]));
                    backupPlan = !backupPlan;
                }

                int threadNum = setting.baseSetting.getThreadNums();
                adjustThreadNum(threads,threadNum,method,&planAddr);
                gatherThreadInfo(threads);
                sleep(5);
            }
            adjustThreadNum(threads,0,nullptr, nullptr);
        }
        bool ReadOneKey(ThreadState *thread) {
            const char *ckey;
            WT_CURSOR *cursor;

            const char*  wprofileName;
            uint32_t whelpfulness1;
            uint32_t whelpfulness2;
            uint32_t wscore;
            uint32_t wtime;
            const char* wsummary;
            const char* wtext;


            int ret = thread->session->open_cursor(thread->session, uri_.c_str(), NULL, NULL, &cursor);


            if (ret != 0) {
                fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
                exit(1);
            }
            int found = 0;
            std::string key(allkeys_.str(rand()%allkeys_.size()));
            cursor->set_key(cursor, key.c_str());
            if (cursor->search(cursor) == 0) {
                found++;
                ret = cursor->get_value(cursor, &wprofileName, &whelpfulness1, &whelpfulness2, &wscore, &wtime, &wsummary, &wtext);
            }

            cursor->close(cursor);
            return found != 0;
        }

        bool WriteOneKey(ThreadState *thread) {

            return true;
        }

        void ReadWhileWriting(leveldb::ThreadState *thread) {

            std::cout << "Thread " << thread->tid << " start!" << std::endl;
            std::unordered_map<uint8_t, bool (WiredTigerBenchmark::*)(ThreadState *thread)> func_map;
            func_map[1] = &WiredTigerBenchmark::ReadOneKey;
            func_map[0] = &WiredTigerBenchmark::WriteOneKey;


            while (thread->STOP.load() == false) {

                std::vector<uint8_t > *plan = (*(thread->which)).load();
                for (auto each : *plan) {

                    (this->*func_map[each])(thread);
                    thread->stats->FinishedSingleOp(each);
                }
            }
            std::cout << "Thread " << thread->tid << " stop!" << std::endl;
        }


        void Crc32c(ThreadState* thread) {
            // Checksum about 500MB of data total
            const int size = 4096;
            const char* label = "(4K per op)";
            std::string data(size, 'x');
            int64_t bytes = 0;
            uint32_t crc = 0;
            while (bytes < 500 * 1048576) {
                crc = crc32c::Value(data.data(), size);
                thread->stats->FinishedSingleOp();
                bytes += size;
            }
            // Print so result is not dead
            fprintf(stderr, "... crc=0x%x\r", static_cast<unsigned int>(crc));

            thread->stats->AddBytes(bytes);
            thread->stats->AddMessage(label);
        }

        void AcquireLoad(ThreadState* thread) {
            int dummy;
            port::AtomicPointer ap(&dummy);
            int count = 0;
            void *ptr = NULL;
            thread->stats->AddMessage("(each op is 1000 loads)");
            while (count < 100000) {
                for (int i = 0; i < 1000; i++) {
                    ptr = ap.Acquire_Load();
                }
                count++;
                thread->stats->FinishedSingleOp();
            }
            if (ptr == NULL) exit(1); // Disable unused variable warning.
        }

        void SnappyCompress(ThreadState* thread) {
            RandomGenerator gen(setting);
            Slice input = gen.Generate(4096);
            int64_t bytes = 0;
            int64_t produced = 0;
            bool ok = true;
            std::string compressed;
            while (ok && bytes < 1024 * 1048576) {  // Compress 1G
                ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
                produced += compressed.size();
                thread->stats->FinishedSingleOp();
                bytes += input.size();
            }

            if (!ok) {
                thread->stats->AddMessage("(snappy failure)");
            } else {
                char buf[100];
                snprintf(buf, sizeof(buf), "(output: %.1f%%)",
                         (produced * 100.0) / bytes);
                thread->stats->AddMessage(buf);
                thread->stats->AddBytes(bytes);
            }
        }

        void SnappyUncompress(ThreadState* thread) {
            RandomGenerator gen(setting);
            Slice input = gen.Generate(4096);
            std::string compressed;
            bool ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
            int64_t bytes = 0;
            char* uncompressed = new char[input.size()];
            while (ok && bytes < 1024 * 1048576) {  // Compress 1G
                ok =  port::Snappy_Uncompress(compressed.data(), compressed.size(),
                                              uncompressed);
                thread->stats->FinishedSingleOp();
                bytes += input.size();
            }
            delete[] uncompressed;

            if (!ok) {
                thread->stats->AddMessage("(snappy failure)");
            } else {
                thread->stats->AddBytes(bytes);
            }
        }

        /* Start Wired Tiger modified section. */
        void Open() {
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
            options.write_buffer_size = setting.FLAGS_write_buffer_size;
            */
#ifndef SYMAS_CONFIG
            config << ",extensions=[libwiredtiger_snappy.so]";
#endif
            //config << ",verbose=[lsm]";
           // std::cout <<"------------" <<setting.FLAGS_db << std::endl;
            Env::Default()->CreateDir(setting.FLAGS_db);
            std::cout << "Config:" << config.str().c_str() << std::endl;
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
                config << "key_format=S,value_format=SLLLLSS";
                config << ",columns=[product_userId, profileName, helpfulness1, helpfulness2, score, time, summary, text]";
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

        void WriteSeq(ThreadState* thread) {
            DoWrite(thread, true);
        }

        void WriteRandom(ThreadState* thread) {
            DoWrite(thread, false);
        }

        void DoWrite(ThreadState* thread, bool seq) {
            if (num_ != setting.FLAGS_num) {
                char msg[100];
                snprintf(msg, sizeof(msg), "(%d ops)", num_);
                thread->stats->AddMessage(msg);
            }

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
            if (seq && setting.FLAGS_threads == 1)
                cur_config << ",bulk=true";

            int ret = thread->session->open_cursor(thread->session, uri_.c_str(), NULL, cur_config.str().c_str(), &cursor);
            if (ret != 0) {
                fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
                exit(1);
            }

            std::ifstream ifs(setting.FLAGS_resource_data);
            std::string str;

            int64_t avg = setting.FLAGS_num/setting.FLAGS_threads;
            int64_t copyavg = avg;
            int offset = setting.shuff[thread->tid];
            if (avg != setting.FLAGS_num) {
                if (offset != 0) {
                    int64_t skip = offset * avg;
                    if (skip != 0) {
                        while(getline(ifs, str)) {
                            if (str.find("review/text:") == 0) {
                                skip --;
                                if (skip == 0)
                                    break;
                            }
                        }
                    }
                }
            }

            std::string key1;
            std::string key2;
            TestRow recRow;
            int64_t writen = 0;

            while(getline(ifs, str) && avg != 0) {
                if (str.find("product/productId:") == 0) {
                    key1 = str.substr(19);
                    continue;
                }
                if (str.find("review/userId:") == 0) {
                    key2 = str.substr(15);
                    continue;
                }
                if (str.find("review/profileName:") == 0) {
                    recRow.profileName = str.substr(20);
                    continue;
                }
                if (str.find("review/helpfulness:") == 0) {
                    char* pos2 = NULL;
                    recRow.helpfulness1 = strtol(str.data()+20, &pos2, 10);
                    recRow.helpfulness2 = strtol(pos2+1, NULL, 10);
                    continue;
                }
                if (str.find("review/score:") == 0) {
                    recRow.score = atol(str.substr(14).c_str());
                    continue;
                }
                if (str.find("review/time:") == 0) {
                    recRow.time = atol(str.substr(13).c_str());
                    continue;
                }
                if (str.find("review/summary:") == 0) {
                    recRow.summary = str.substr(16);
                    continue;
                }
                if (str.find("review/text:") == 0) {
                    recRow.text = str.substr(13);
                    recRow.product_userId = key1 + " " + key2;

                    cursor->set_key(cursor, recRow.product_userId.c_str());
                    cursor->set_value(cursor, recRow.profileName.c_str(), recRow.helpfulness1, recRow.helpfulness2, recRow.score, recRow.time, recRow.summary.c_str(), recRow.text.c_str());
                    int ret = cursor->insert(cursor);
                    if (ret != 0) {
                        fprintf(stderr, "set error: %s\n", wiredtiger_strerror(ret));
                        exit(1);
                    }
                    thread->stats->FinishedSingleOp();
                    writen ++;
                    avg --;
                    continue;
                }
            }
            cursor->close(cursor);
            time_t now;
            struct tm *timenow;
            time(&now);
            timenow = localtime(&now);
            printf("writenum %lld, avg %lld, offset %d, time %s\n",writen, copyavg, offset, asctime(timenow));
        }
        void DoWrite(bool seq) {
            std::cout << "Do write now:" << allkeys_.size() << std::endl;
            if (num_ != setting.FLAGS_num) {
                char msg[100];
                snprintf(msg, sizeof(msg), "(%d ops)", num_);
            }

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
            //if (seq)
              //  cur_config << ",bulk=true";


            WT_SESSION * session ;
            conn_->open_session(conn_, NULL, NULL, &session);
            assert(session != NULL);
            int ret = session->open_cursor(session, uri_.c_str(), NULL, cur_config.str().c_str(), &cursor);
            if (ret != 0) {
                fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
                exit(1);
            }

            std::ifstream ifs(setting.FLAGS_resource_data);
            std::string str;

            int64_t avg = setting.FLAGS_num;
            int64_t copyavg = avg;


            std::string key1;
            std::string key2;
            TestRow recRow;
            int64_t writen = 0;

            while(getline(ifs, str) && avg != 0) {
                if (str.find("product/productId:") == 0) {
                    key1 = str.substr(19);
                    continue;
                }
                if (str.find("review/userId:") == 0) {
                    key2 = str.substr(15);
                    continue;
                }
                if (str.find("review/profileName:") == 0) {
                    recRow.profileName = str.substr(20);
                    continue;
                }
                if (str.find("review/helpfulness:") == 0) {
                    char* pos2 = NULL;
                    recRow.helpfulness1 = strtol(str.data()+20, &pos2, 10);
                    recRow.helpfulness2 = strtol(pos2+1, NULL, 10);
                    continue;
                }
                if (str.find("review/score:") == 0) {
                    recRow.score = atol(str.substr(14).c_str());
                    continue;
                }
                if (str.find("review/time:") == 0) {
                    recRow.time = atol(str.substr(13).c_str());
                    continue;
                }
                if (str.find("review/summary:") == 0) {
                    recRow.summary = str.substr(16);
                    continue;
                }
                if (str.find("review/text:") == 0) {
                    recRow.text = str.substr(13);
                    recRow.product_userId = key1 + " " + key2;

                    cursor->set_key(cursor, recRow.product_userId.c_str());
                    cursor->set_value(cursor, recRow.profileName.c_str(), recRow.helpfulness1, recRow.helpfulness2, recRow.score, recRow.time, recRow.summary.c_str(), recRow.text.c_str());
                    int ret = cursor->insert(cursor);
                    if (ret != 0) {
                        fprintf(stderr, "set error: %s\n", wiredtiger_strerror(ret));
                        exit(1);
                    }
                    writen ++;
                    avg --;
                    continue;
                }
            }
            cursor->close(cursor);
            time_t now;
            struct tm *timenow;
            time(&now);
            timenow = localtime(&now);
            std::cout << "writenum " << writen << " time " << asctime(timenow) << std::endl;
        }


        void DoWrite(ThreadState* thread, bool seq,int times) {
            if (num_ != setting.FLAGS_num) {
                char msg[100];
                snprintf(msg, sizeof(msg), "(%d ops)", num_);
                thread->stats->AddMessage(msg);
            }

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
            if (seq && setting.FLAGS_threads == 1)
                cur_config << ",bulk=true";

            int ret = thread->session->open_cursor(thread->session, uri_.c_str(), NULL, cur_config.str().c_str(), &cursor);
            if (ret != 0) {
                fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
                exit(1);
            }

            std::ifstream ifs(setting.FLAGS_resource_data);
            std::string str;

            int64_t avg = setting.FLAGS_num/setting.FLAGS_threads;
            int64_t copyavg = avg;
            int offset = setting.shuff[thread->tid];
            if (avg != setting.FLAGS_num) {
                if (offset != 0) {
                    int64_t skip = offset * avg;
                    if (skip != 0) {
                        while(getline(ifs, str)) {
                            if (str.find("review/text:") == 0) {
                                skip --;
                                if (skip == 0)
                                    break;
                            }
                        }
                    }
                }
            }

            std::string key1;
            std::string key2;
            TestRow recRow;
            int64_t writen = 0;

            while(getline(ifs, str) && times != 0) {
                times--;
                if (str.find("product/productId:") == 0) {
                    key1 = str.substr(19);
                    continue;
                }
                if (str.find("review/userId:") == 0) {
                    key2 = str.substr(15);
                    continue;
                }
                if (str.find("review/profileName:") == 0) {
                    recRow.profileName = str.substr(20);
                    continue;
                }
                if (str.find("review/helpfulness:") == 0) {
                    char* pos2 = NULL;
                    recRow.helpfulness1 = strtol(str.data()+20, &pos2, 10);
                    recRow.helpfulness2 = strtol(pos2+1, NULL, 10);
                    continue;
                }
                if (str.find("review/score:") == 0) {
                    recRow.score = atol(str.substr(14).c_str());
                    continue;
                }
                if (str.find("review/time:") == 0) {
                    recRow.time = atol(str.substr(13).c_str());
                    continue;
                }
                if (str.find("review/summary:") == 0) {
                    recRow.summary = str.substr(16);
                    continue;
                }
                if (str.find("review/text:") == 0) {
                    recRow.text = str.substr(13);
                    recRow.product_userId = key1 + " " + key2;

                    cursor->set_key(cursor, recRow.product_userId.c_str());
                    cursor->set_value(cursor, recRow.profileName.c_str(), recRow.helpfulness1, recRow.helpfulness2, recRow.score, recRow.time, recRow.summary.c_str(), recRow.text.c_str());
                    int ret = cursor->insert(cursor);
                    if (ret != 0) {
                        fprintf(stderr, "set error: %s\n", wiredtiger_strerror(ret));
                        exit(1);
                    }
                    thread->stats->FinishedSingleOp();
                    writen ++;
                    avg --;
                    continue;
                }
            }
            cursor->close(cursor);
            time_t now;
            struct tm *timenow;
            time(&now);
            timenow = localtime(&now);
         //   printf("writenum %lld, avg %lld, offset %d, time %s\n",writen, copyavg, offset, asctime(timenow));
        }
        void ReadSequential(ThreadState* thread) {
            const char *ckey, *cvalue;
            char key[100];
            WT_CURSOR *cursor;
            int ret = thread->session->open_cursor(thread->session, uri_.c_str(), NULL, NULL, &cursor);
            if (ret != 0) {
                fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
                exit(1);
            }

            int64_t bytes = 0;
            int i = 0;
            int k;
            if (setting.FLAGS_stagger) {
                k = (setting.FLAGS_num / setting.FLAGS_threads) * thread->tid + 1;
                snprintf(key, sizeof(key), "%016d", k);
                cursor->set_key(cursor, key);
                if (cursor->search(cursor) != 0) {
                    fprintf(stderr, "cursor search for key %d error: %s\n", k, wiredtiger_strerror(ret));
                    exit(1);
                }
            }
            repeat:
            while ((ret = cursor->next(cursor)) == 0 && i < reads_) {
                cursor->get_key(cursor, &ckey);
                cursor->get_value(cursor, &cvalue);
                thread->stats->FinishedSingleOp();
                bytes += strlen(ckey) + strlen(cvalue);
                ++i;
            }
            /*
             * Allow repetitive reads, simply wrapping back if the number of
             * reads exceeds the number of keys to read.
             */
            sscanf(ckey, "%d", &k);
            if (k == (setting.FLAGS_num - 1) && i < reads_ && cursor->reset(cursor) == 0)
                goto repeat;
            if (ret != 0) {
                fprintf(stderr, "ReadSeq: cursor_next ckey %s k %d i %d error: %s\n", ckey, k, i, wiredtiger_strerror(ret));
                exit(1);
            }

            cursor->close(cursor);
            thread->stats->AddBytes(bytes);
        }

        void ReadReverse(ThreadState* thread) {
            const char *ckey, *cvalue;
            char key[100];
            WT_CURSOR *cursor;
            int ret = thread->session->open_cursor(thread->session, uri_.c_str(), NULL, NULL, &cursor);
            if (ret != 0) {
                fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
                exit(1);
            }

            int64_t bytes = 0;
            int i = 0;
            int k;
            if (setting.FLAGS_stagger) {
                k = (setting.FLAGS_num / setting.FLAGS_threads) * thread->tid + 1;
                snprintf(key, sizeof(key), "%016d", k);
                cursor->set_key(cursor, key);
                if (cursor->search(cursor) != 0) {
                    fprintf(stderr, "cursor search for key %d error: %s\n", k, wiredtiger_strerror(ret));
                    exit(1);
                }
                // Make sure ckey has a valid value in it in case we are at the front
                // and the while loop below is skipped initially.
                cursor->get_key(cursor, &ckey);
            }
            repeat:
            while ((ret = cursor->prev(cursor)) == 0 && i < reads_) {
                cursor->get_key(cursor, &ckey);
                cursor->get_value(cursor, &cvalue);
                thread->stats->FinishedSingleOp();
                bytes += strlen(ckey) + strlen(cvalue);
                ++i;
            }
            /*
             * Allow repetitive reads, simply wrapping back if the number of
             * reads exceeds the number of keys to read.
             */
            sscanf(ckey, "%d", &k);
            if (k == 1 && i < reads_ && cursor->reset(cursor) == 0)
                goto repeat;

            if (ret != 0) {
                fprintf(stderr, "ReadReverse: cursor_next ckey %s k %d i %d error: %s\n", ckey, k, i, wiredtiger_strerror(ret));
                exit(1);
            }
            cursor->close(cursor);
            thread->stats->AddBytes(bytes);
        }

        void ReadRandom(ThreadState* thread) {
            const char *ckey;
            WT_CURSOR *cursor;

            const char*  wprofileName;
            uint32_t whelpfulness1;
            uint32_t whelpfulness2;
            uint32_t wscore;
            uint32_t wtime;
            const char* wsummary;
            const char* wtext;
            int ret = thread->session->open_cursor(thread->session, uri_.c_str(), NULL, NULL, &cursor);
            if (ret != 0) {
                fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
                exit(1);
            }
            int found = 0;

            int *shuffr = NULL;
            shuffr = (int *)malloc(setting.FLAGS_num * sizeof(int));
            for (int i=0; i<setting.FLAGS_num; i++)
                shuffr[i] = i;
            thread->rand.Shuffle(shuffr, setting.FLAGS_num);
            for (int i = 0; i < reads_; i++) {
                int k = shuffr[i];
                std::string key = allkeys_.str(k);
                cursor->set_key(cursor, key.c_str());
                if (cursor->search(cursor) == 0) {
                    found++;
                    ret = cursor->get_value(cursor, &wprofileName, &whelpfulness1, &whelpfulness2, &wscore, &wtime, &wsummary, &wtext);
                }
                thread->stats->FinishedSingleOp();
            }
            cursor->close(cursor);
            char msg[100];
            snprintf(msg, sizeof(msg), "(%d found) %d", found, setting.FLAGS_num);
            thread->stats->AddMessage(msg);
        }

        void ReadMissing(ThreadState* thread) {
            const char *ckey;
            WT_CURSOR *cursor;
            int ret = thread->session->open_cursor(thread->session, uri_.c_str(), NULL, NULL, &cursor);
            if (ret != 0) {
                fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
                exit(1);
            }
            for (int i = 0; i < reads_; i++) {
                char key[100];
                const int k = thread->rand.Next() % setting.FLAGS_num;
                snprintf(key, sizeof(key), "%016d.", k);
                cursor->set_key(cursor, key);
                cursor->search(cursor);
                thread->stats->FinishedSingleOp();
            }
            cursor->close(cursor);
        }

        void ReadHot(ThreadState* thread) {
            const char *ckey;
            WT_CURSOR *cursor;

            const char* wproductId;
            const char* wuserId;
            const char*  wprofileName;
            uint32_t whelpfulness1;
            uint32_t whelpfulness2;
            uint32_t wscore;
            uint32_t wtime;
            const char* wsummary;
            const char* wtext;

            int ret = thread->session->open_cursor(thread->session, uri_.c_str(), NULL, NULL, &cursor);
            if (ret != 0) {
                fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
                exit(1);
            }
            const int range = (setting.FLAGS_num + 99) / 100;
            for (int i = 0; i < reads_; i++) {
                const int k = thread->rand.Next() % range;
                uint64_t key = k + 1;
                cursor->set_key(cursor, key);
                if (cursor->search(cursor) == 0) {
                    ret = cursor->get_value(cursor, &wproductId, &wuserId, &wprofileName, &whelpfulness1, &whelpfulness2, &wscore, &wtime, &wsummary, &wtext);
                }
                thread->stats->FinishedSingleOp();
            }
            cursor->close(cursor);
        }

        void SeekRandom(ThreadState* thread) {
            const char *ckey;
            WT_CURSOR *cursor;
            int ret = thread->session->open_cursor(thread->session, uri_.c_str(), NULL, NULL, &cursor);
            if (ret != 0) {
                fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
                exit(1);
            }
            int found = 0;
            for (int i = 0; i < reads_; i++) {
                char key[100];
                const int k = thread->rand.Next() % setting.FLAGS_num;
                snprintf(key, sizeof(key), "%016d", k);
                cursor->set_key(cursor, key);
                if(cursor->search(cursor) == 0) {
                    found++;
                }
                thread->stats->FinishedSingleOp();
            }
            cursor->close(cursor);
            char msg[100];
            snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
            thread->stats->AddMessage(msg);
        }

        void DoDelete(ThreadState* thread, bool seq) {
            const char *ckey;
            int64_t bytes = 0;
            WT_CURSOR *cursor;
            int ret = thread->session->open_cursor(thread->session, uri_.c_str(), NULL, NULL, &cursor);
            if (ret != 0) {
                fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
                exit(1);
            }
            std::stringstream txn_config;
            txn_config.str("");
            txn_config << "isolation=snapshot";
            if (sync_)
                txn_config << ",sync=full";
            else
                txn_config << ",sync=none";
            for (int i = 0; i < num_; i += entries_per_batch_) {
                for (int j = 0; j < entries_per_batch_; j++) {
#ifdef RAND_SHUFFLE
                    int k = seq ? i+j : setting.shuff[i+j];
#else
                    int k = seq ? i+j : (thread->rand.Next() % setting.FLAGS_num);
#endif
                    char key[100];
                    snprintf(key, sizeof(key), "%016d", k);
                    if (k == 0)
                        continue; /* Wired Tiger does not support 0 keys. */
                    cursor->set_key(cursor, key);
                    if ((ret = cursor->remove(cursor)) != 0) {
                        if (setting.FLAGS_threads == 1 || ret != WT_NOTFOUND) {
                            fprintf(stderr, "del error: key %s %s\n", key, wiredtiger_strerror(ret));
                            exit(1);
                        }
                    }
                    thread->stats->FinishedSingleOp();
                    bytes += strlen(key);
                }
            }
            cursor->close(cursor);
            thread->stats->AddBytes(bytes);
        }

        void DeleteSeq(ThreadState* thread) {
            DoDelete(thread, true);
        }

        void DeleteRandom(ThreadState* thread) {
            DoDelete(thread, false);
        }

        void ReadWriteDel(ThreadState* thread) {
            if (thread->tid % 3 == 0) { // read
                ReadRandom(thread);
            } else if (thread->tid % 3 == 1) { // write
                int64_t num = 0;
                while(true) {
                    WT_CURSOR *cursor;
                    std::stringstream cur_config;
                    cur_config.str("");
                    cur_config << "overwrite";

                    int ret = thread->session->open_cursor(thread->session, uri_.c_str(), NULL, cur_config.str().c_str(), &cursor);
                    if (ret != 0) {
                        fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
                        exit(1);
                    }

                    std::ifstream ifs(setting.FLAGS_resource_data);
                    std::string str;
                    std::string key1;
                    std::string key2;
                    TestRow recRow;

                    while(getline(ifs, str)) {
                        if (str.find("product/productId:") == 0) {
                            key1 = str.substr(19);
                        }
                        if (str.find("review/userId:") == 0) {
                            key2 = str.substr(15);
                        }
                        if (str.find("review/profileName:") == 0) {
                            recRow.profileName = str.substr(20);
                        }
                        if (str.find("review/helpfulness:") == 0) {
                            char* pos2 = NULL;
                            recRow.helpfulness1 = strtol(str.data()+20, &pos2, 10);
                            recRow.helpfulness2 = strtol(pos2+1, NULL, 10);
                        }
                        if (str.find("review/score:") == 0) {
                            recRow.score = atol(str.substr(14).c_str());
                        }
                        if (str.find("review/time:") == 0) {
                            recRow.time = atol(str.substr(13).c_str());
                        }
                        if (str.find("review/summary:") == 0) {
                            recRow.summary = str.substr(16);
                        }
                        if (str.find("review/text:") == 0) {
                            recRow.text = str.substr(13);
                            recRow.product_userId = key1 + " " + key2;
                            cursor->set_key(cursor, recRow.product_userId.c_str());
                            cursor->set_value(cursor, recRow.profileName.c_str(), recRow.helpfulness1, recRow.helpfulness2, recRow.score, recRow.time, recRow.summary.c_str(), recRow.text.c_str());
                            int ret = cursor->insert(cursor);
                            if (ret != 0) {
                                fprintf(stderr, "set error: %s\n", wiredtiger_strerror(ret));
                                exit(1);
                            }
                            num ++;
                        }
                        MutexLock l(&thread->shared->mu);
                        if (thread->shared->num_done + 2*setting.FLAGS_threads/3 >= thread->shared->num_initialized) {
                            printf("extra write operations number %d\n", num);
                            cursor->close(cursor);
                            return;
                        }
                    }
                }
            } else {  // del
                int64_t num = 0;
                WT_CURSOR *cursor;
                int ret = thread->session->open_cursor(thread->session, uri_.c_str(), NULL, NULL, &cursor);
                if (ret != 0) {
                    fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
                    exit(1);
                }

                while(true) {
                    const int k = thread->rand.Next() % setting.FLAGS_num;
                    cursor->set_key(cursor, allkeys_.at(k));
                    ret = cursor->remove(cursor);
                    num ++;
                    MutexLock l(&thread->shared->mu);
                    if (thread->shared->num_done + 2*setting.FLAGS_threads/3 >= thread->shared->num_initialized) {
                        printf("extra del operations number %d\n", num);
                        break;
                    }
                }
                cursor->close(cursor);
            }
        }

        void ReadWhileWritingNew(ThreadState* thread) {
            terark::AutoFree<int> shuffrw(setting.FLAGS_num);
            terark::AutoFree<int> shuffr(setting.FLAGS_num);
            int read_num = int(setting.FLAGS_num * setting.FLAGS_read_write_percent / 100.0);
            std::fill_n(shuffrw.p , read_num, 1);
            std::fill_n(shuffrw.p + read_num, setting.FLAGS_num-read_num, 0);
            for (int i=0; i<setting.FLAGS_num; i++) {
                shuffr[i] = i;
            }

            int64_t readn = 0;
            int64_t writen = 0;
            thread->rand.Shuffle(shuffrw, setting.FLAGS_num);
            thread->rand.Shuffle(shuffr, setting.FLAGS_num);

            const char*  wprofileName;
            uint32_t whelpfulness1;
            uint32_t whelpfulness2;
            uint32_t wscore;
            uint32_t wtime;
            const char* wsummary;
            const char* wtext;

            WT_CURSOR *cursor;
            std::stringstream cur_config;
            cur_config.str("");
            cur_config << "overwrite";
            int ret = thread->session->open_cursor(thread->session, uri_.c_str(), NULL, cur_config.str().c_str(), &cursor);
            if (ret != 0) {
                fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
                exit(1);
            }
            int found = 0;
            std::ifstream ifs(setting.FLAGS_resource_data);
            std::string str;


            int64_t avg = setting.FLAGS_num/setting.FLAGS_threads;
            int64_t copyavg = avg;
            int offset = setting.shuff[thread->tid];
            if (avg != setting.FLAGS_num) {
                if (offset != 0) {
                    int64_t skip = offset * avg;
                    if (skip != 0) {
                        while(getline(ifs, str)) {
                            if (str.find("review/text:") == 0) {
                                skip --;
                                if (skip == 0)
                                    break;
                            }
                        }
                    }
                }
            }

            std::string key1;
            std::string key2;
            TestRow recRow;

            for (int i=0; i<setting.FLAGS_reads; i++) {
                if (shuffrw[i] == 1) {
                    // read
                    int k = shuffr[i];
                    std::string key = allkeys_.str(k);
                    cursor->set_key(cursor, key.c_str());
                    if (cursor->search(cursor) == 0) {
                        found++;
                        ret = cursor->get_value(cursor, &wprofileName, &whelpfulness1, &whelpfulness2, &wscore, &wtime, &wsummary, &wtext);
                    }
                    readn ++;
                    thread->stats->FinishedSingleOp();
                } else {
                    // write
                    while(getline(ifs, str) && avg != 0) {
                        if (str.find("product/productId:") == 0) {
                            key1 = str.substr(19);
                            continue;
                        }
                        if (str.find("review/userId:") == 0) {
                            key2 = str.substr(15);
                            continue;
                        }
                        if (str.find("review/profileName:") == 0) {
                            recRow.profileName = str.substr(20);
                            continue;
                        }
                        if (str.find("review/helpfulness:") == 0) {
                            char* pos2 = NULL;
                            recRow.helpfulness1 = strtol(str.data()+20, &pos2, 10);
                            recRow.helpfulness2 = strtol(pos2+1, NULL, 10);
                            continue;
                        }
                        if (str.find("review/score:") == 0) {
                            recRow.score = atol(str.substr(14).c_str());
                            continue;
                        }
                        if (str.find("review/time:") == 0) {
                            recRow.time = atol(str.substr(13).c_str());
                            continue;
                        }
                        if (str.find("review/summary:") == 0) {
                            recRow.summary = str.substr(16);
                            continue;
                        }
                        if (str.find("review/text:") == 0) {
                            recRow.text = str.substr(13);
                            recRow.product_userId = key1 + " " + key2;

                            cursor->set_key(cursor, recRow.product_userId.c_str());
                            cursor->set_value(cursor, recRow.profileName.c_str(), recRow.helpfulness1, recRow.helpfulness2, recRow.score, recRow.time, recRow.summary.c_str(), recRow.text.c_str());
                            ret = cursor->insert(cursor);
                            if (ret != 0) {
                                fprintf(stderr, "set error: %s\n", wiredtiger_strerror(ret));
                                exit(1);
                            }
                            writen ++;
                            avg --;
                            thread->stats->FinishedSingleOp();
                            break;
                        }
                    }
                }
            }
            cursor->close(cursor);
            printf("readnum %lld, writenum %lld, avg %lld, offset %d\n", readn, writen, copyavg, offset);
        }

        void Compact(ThreadState* thread) {
            /*
             * TODO: It probably makes sense to run multiple compacts in parallel
             * if there are multiple LSM trees. The code to do that involves
             * opening multiple session handles (and possibly multiple threads), so
             * don't do that for now.
             */
            WT_CURSOR *md_cursor;
            int exact, ret;
            const char *key;
            char buf[100]; // For the timeout.

            if (!setting.FLAGS_use_lsm)
                return ;

            if ((ret = thread->session->open_cursor(
                    thread->session, "metadata:", NULL, NULL, &md_cursor)) != 0) {
                fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
                exit(1);
            }
            md_cursor->set_key(md_cursor, "lsm:");
            for (ret = md_cursor->search_near(md_cursor, &exact);
                 ret == 0; ret = md_cursor->next(md_cursor)) {
                /* If the initial search result is behind, move our cursor forwards */
                if (exact == -1) {
                    ret = md_cursor->next(md_cursor);
                    exact = 1;
                }
                ret = md_cursor->get_key(md_cursor, &key);
                if (ret != 0 || strncmp(key, "lsm:", 4) != 0)
                    break;

                // TODO: track how long we've been waiting if there are multiple
                // LSM trees to compact.
                snprintf(buf, 100, "timeout=%d", setting.FLAGS_max_compact_wait);
                // Run a compact on this LSM.
                if ((ret = thread->session->compact(thread->session, key, buf)) != 0 &&
                    ret != ETIMEDOUT) {
                    fprintf(stderr, "compact error: %s\n", wiredtiger_strerror(ret));
                    exit(1);
                }
            }
            md_cursor->close(md_cursor);
        }

        void PrintStats(const char* key) {
            /* TODO: Implement two different cases based on input string. */
            const char *ckey, *cvalue;
            WT_CURSOR *cursor;
            std::stringstream suri;
            suri.str("");
            suri << "statistics:" << uri_;
            WT_SESSION *session;
            conn_->open_session(conn_, NULL, NULL, &session);
            int ret = session->open_cursor(session, suri.str().c_str(), NULL, NULL, &cursor);
            if (ret != 0) {
                fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
                exit(1);
            }

            const char *desc, *pvalue;
            uint64_t value;
            while(cursor->next(cursor) == 0 &&
                  cursor->get_value(cursor, &desc, &pvalue, &value) == 0)
                printf("\t%s=%s\n", desc, pvalue);
            session->close(session, NULL);
        }

        /* Only used by HeapProfile.
        static void WriteToFile(void* arg, const char* buf, int n) {
          reinterpret_cast<WritableFile*>(arg)->Append(Slice(buf, n));
        }
        */

        void HeapProfile() {
            fprintf(stderr, "heap profiling not supported\n");
            return;
            /*
            char fname[100];
            snprintf(fname, sizeof(fname), "%s/heap-%04d", setting.FLAGS_db, ++heap_counter_);
            WritableFile* file;
            Status s = Env::Default()->NewWritableFile(fname, &file);
            if (!s.ok()) {
              fprintf(stderr, "%s\n", s.ToString().c_str());
              return;
            }
            bool ok = port::GetHeapProfile(WriteToFile, file);
            delete file;
            if (!ok) {
              fprintf(stderr, "heap profiling not supported\n");
              Env::Default()->DeleteFile(fname);
            }
            */
        }

        void updatePlan(std::vector<uint8_t> &plan, uint8_t readPercent) {
            assert(readPercent <= 100);
            if (plan.size() != 100){
                plan.resize(100);
            }
            std::fill_n(plan.begin(),readPercent,1);
            std::fill(plan.begin()+readPercent,plan.end(),0);
            std::shuffle(plan.begin(),plan.end(),std::default_random_engine());
        }
    };

}  // namespace leveldb


#endif //TERARKDB_TEST_FRAMEWORK_WIREDTIGERBENCHMARK_H
