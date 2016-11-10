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
#include <src/TerarkRocksDbBenchmark.h>
#include <src/PosixBenchmark.h>
#include <src/Stats.h>
#include <src/analysis_worker.h>

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

void sigint_fuc(int sig) {
    std::cout << "Ctrl+c" << std::endl;
    g_settings->setStop();
    g_worker->stop();
    g_io_service->stop();
}

int main(int argc, char **argv) {
    //Stats::readTimeDataCq
    signal(SIGINT, sigint_fuc);
    if (argc < 2) {
        fprintf(stderr, "WiredTiger or Terark?");
    }
	char *passwd = getenv("MYSQL_PASSWD");
    Setting setting(argc, argv, argv[1]);
    g_settings = &setting;
    std::unique_ptr<Benchmark> bm;

    if (strcmp(argv[1], "terarkdb") == 0) {
        bm.reset(new TerarkBenchmark(setting));
        g_worker = new AnalysisWorker("terarkdb",&setting);
    }
    else if (strcmp(argv[1], "wiredtiger") == 0) {
        bm.reset(new WiredTigerBenchmark(setting));
        g_worker = new AnalysisWorker("wiredtiger",&setting);
    }
    else if (strcmp(argv[1], "compact") == 0) {
        compact(setting);
        puts("Compact Finish.Exit the program.");
        return 0;
    }
    else if (strcmp(argv[1], "rocksdb") == 0) {
        bm.reset(new RocksDbBenchmark(setting));
        g_worker = new AnalysisWorker("rocksdb", &setting);
    }
    else if (strcmp(argv[1],"terark_rocksdb") == 0) {
        bm.reset(new TerarkRocksDbBenchmark(setting));
        g_worker = new AnalysisWorker("terocksdb", &setting);
    }
    else if (strcmp(argv[1],"posix") == 0){
        bm.reset(new PosixBenchmark(setting));
        g_worker = new AnalysisWorker("posix", &setting);
    }
    //start a thread for tcp server
    boost::asio::io_service io_service;
    g_io_service = &io_service;
    std::thread tcpServerThread(tcpServer, &setting, bm.get());
    // start a thread for analysis and data upload
    std::thread workerThrad([](AnalysisWorker* w) {
        w->run();
    }, g_worker);
    if (bm) {
        bm->Run();
    }
    tcpServerThread.join();
    workerThrad.join();
    if (g_worker) {
        delete g_worker;
        g_worker = NULL;
    }
    return 0;
}
