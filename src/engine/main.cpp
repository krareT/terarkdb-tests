#include <sstream>
#include <iostream>
#include <fstream>
#include <string.h>
#include <db/db_impl.h>
#include <db/version_set.h>
#include <boost/asio.hpp>
#include <src/Setting.h>
#include <src/Benchmark.h>
#include <src/TcpServer.h>
#include <signal.h>
#include <src/TerarkBenchmark.h>
#include <src/WiredTigerBenchmark.h>
#include <src/RocksDbBenchmark.h>
#include <src/TerocksBenchmark.h>
#include <src/PosixBenchmark.h>
#include <src/Stats.h>
#include <src/analysis_worker.h>

using terark::fstring;

boost::asio::io_service* g_io_service;
void tcpServer(Setting *setting, Benchmark *bm) {
    std::cout << "------------Tcp Server start-------------" << std::endl;
    try {
        Server server(*g_io_service, 6666, *setting, bm);
        g_io_service->run();
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

Setting *g_settings;
AnalysisWorker *g_worker;

void stop_test() {
  g_settings->setStop();
  g_worker->stop();
  g_io_service->stop();
}

void sigint_fuc(int sig) {
    fprintf(stderr, "Ctrl + C\n");
    stop_test();
}

int main(int argc, char **argv) {
    //Stats::readTimeDataCq
    signal(SIGINT, sigint_fuc);
    if (argc < 2) {
        fprintf(stderr, "usage: %s WhichDB options\n", argv[0]);
        return 1;
    }
    const fstring whichDB = argv[1];
    Setting setting(argc, argv);
    g_settings = &setting;
    std::unique_ptr<Benchmark> bm;

    g_worker = new AnalysisWorker(&setting);

    g_worker->engine_name = "terarkdb";
    if (whichDB == "terarkdb") {
        bm.reset(new TerarkBenchmark(setting));
    }
    else if (whichDB == "wiredtiger") {
        bm.reset(new WiredTigerBenchmark(setting));
    }
    else if (whichDB == "compact") {
        compact(setting);
        puts("Compact Finish.Exit the program.");
        return 0;
    }
    else if (whichDB == "rocksdb") {
        bm.reset(new RocksDbBenchmark(setting));
    }
    else if (whichDB == "terark_rocksdb" || whichDB == "terocksdb" ) {
        bm.reset(new TerarkRocksDbBenchmark(setting));
        g_worker->engine_name = "terocksdb";
    }
    else if (whichDB == "posix") {
        bm.reset(new PosixBenchmark(setting));
    }
    if (!setting.alt_engine_name.empty()) {
        g_worker->engine_name = setting.alt_engine_name;
    }
    //start a thread for tcp server
    boost::asio::io_service io_service;
    g_io_service = &io_service;
    std::thread tcpServerThread(tcpServer, &setting, bm.get());
    // start a thread for analysis and data upload
    std::thread workerThread([](AnalysisWorker* w){w->run();}, g_worker);
    if (bm) {
        bm->Run();
    }
    tcpServerThread.join();
    workerThread.join();
    if (g_worker) {
        delete g_worker;
        g_worker = NULL;
    }
    return 0;
}

