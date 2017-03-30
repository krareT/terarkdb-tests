#include <signal.h>
#include <string.h>
#include <boost/asio.hpp>
#include <src/Setting.h>
#include <src/TcpServer.h>
#include <src/Stats.h>
#include <src/analysis_worker.h>

using terark::fstring;

boost::asio::io_service* g_io_service;
void tcpServer(Setting *setting, Benchmark *bm) {
    fprintf(stderr, "------------Tcp Server start-------------\n");
    try {
        Server server(*g_io_service, 6666, *setting, bm);
        g_io_service->run();
    }
    catch (std::exception &e) {
        fprintf(stderr, "ERROR: %s\n", e.what());
    }
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

char** g_argv;
int g_argc;

int main(int argc, char **argv) {
    fprintf(stderr, "%s is compiled at %s %s\n", argv[0], __DATE__, __TIME__);
    signal(SIGINT, sigint_fuc);
    if (argc < 2) {
        fprintf(stderr, "usage: %s WhichDB options\n", argv[0]);
        return 1;
    }
    g_argv = argv;
    g_argc = argc;
    Setting::BenchmarkName = default_engine_name;
    Setting setting(argc, argv);
    g_settings = &setting;
    g_worker = new AnalysisWorker(&setting);
    g_worker->engine_name = default_engine_name;
    if (!setting.alt_engine_name.empty()) {
        g_worker->engine_name = setting.alt_engine_name;
    }
    std::unique_ptr<Benchmark> bm(new BenchmarkClass(setting));

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

