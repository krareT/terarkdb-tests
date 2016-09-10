#include <sstream>
#include <iostream>
#include <fstream>
#include <string.h>
#include "db/db_impl.h"
#include "db/version_set.h"
#include <boost/asio.hpp>
#include <src/Setting.h>
#include "src/Benchmark.h"
#include "src/TcpServer.h"
#include <signal.h>
#include <src/TerarkBenchmark.h>
#include <src/WiredTigerBenchmark.h>
#include <src/RocksDbBenchmark.h>
#include <src/TerarkRocksDbBenchmark.h>
#include "src/Stats.h"
#include "src/analysis_worker.h"

void tcpServer(Setting *setting, Benchmark *bm) {
    std::cout << "------------Tcp Server start-------------" << std::endl;
    try {
        boost::asio::io_service io_service;
        Server server(io_service, 6666, *setting, bm);
        io_service.run();
    }
    catch (std::exception &e) {
        std::cerr << e.what() << std::endl;
    }
}

void compact(Setting &setting) {
    DbTablePtr tab = DbTable::open(setting.FLAGS_db);
    std::cout << "press any key to compact" << std::endl;
    getchar();
    tab->compact();
    tab->safeStopAndWaitForCompress();
    tab = nullptr;
}

Setting *set;
AnalysisWorker *worker;

void sigint_fuc(int sig) {
    std::cout << "Ctrl+c" << std::endl;
    set->setStop();
}

int main(int argc, char **argv) {
    //Stats::readTimeDataCq
    signal(SIGINT, sigint_fuc);
    if (argc < 2) {
        fprintf(stderr, "WiredTiger or Terark?");
    }
    Setting setting(argc, argv, argv[1]);
    set = &setting;
    Benchmark *bm = nullptr;

    if (strcmp(argv[1], "terarkdb") == 0) {
        bm = new TerarkBenchmark(setting);
        worker = new AnalysisWorker("terarkdb",&setting);
    }
    else if (strcmp(argv[1], "wiredtiger") == 0) {
        bm = new WiredTigerBenchmark(setting);
        worker = new AnalysisWorker("wiredtiger",&setting);
    }
    else if (strcmp(argv[1], "compact") == 0) {
        compact(setting);
        puts("Compact Finish.Exit the program.");
        exit(1);
    } else if (strcmp(argv[1], "rocksdb") == 0) {
        bm = new RocksDbBenchmark(setting);
        worker = new AnalysisWorker("rocksdb", &setting);
    } else if (strcmp(argv[1],"terark_rocksdb") == 0){
        bm = new TerarkRocksDbBenchmark(setting);
        worker = new AnalysisWorker("rocksdb", &setting);
    }
    //start a thread for tcp server
    std::thread tcpServerThread(tcpServer, &setting, bm);
    // start a thread for analysis and data upload
//    std::thread workerThrad([](AnalysisWorker* w) {
//        w->run();
//    }, worker);
    if (nullptr != bm) bm->Run();
    delete bm;
    exit(1);
}
