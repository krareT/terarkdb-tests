

#include <sstream>
#include <iostream>
#include <fstream>
#include <string.h>
#include "db/db_impl.h"
#include "db/version_set.h"
#include <boost/asio.hpp>
#include <terark/db/db_table.hpp>
#include <terark/util/fstrvec.hpp>
#include <src/Setting.h>
#include "src/TerarkBenchmark.h"
#include "src/TcpServer.h"

void tcpServer(Setting *setting,Benchmark *bm){
    std::cout << "------------Tcp Server start-------------" << std::endl;
    try
    {
        boost::asio::io_service io_service;
        Server server(io_service,6666,*setting,bm);
        io_service.run();
    }
    catch (std::exception& e)
    {
        std::cerr << e.what() << std::endl;
    }
}
void compact( Setting &setting){
    auto tab = CompositeTable::open(setting.FLAGS_db);
    std::cout << "press any key to compact" << std::endl;
    getchar();
    tab->compact();
    tab->safeStopAndWaitForCompress();
    tab= nullptr;
}
int main(int argc, char** argv) {

    if (argc < 2){
        fprintf(stderr,"WiredTiger or Terark?");
    }
    Setting setting(argc,argv,argv[1]);
    Benchmark *bm;
    if (strcmp(argv[1],"Terark") == 0) {
        bm = new TerarkBenchmark(setting);
    }
    else if (strcmp(argv[1],"WiredTiger") == 0){
        bm = new  WiredTigerBenchmark(setting);
    }
    else if (strcmp(argv[1],"compact") == 0){
        compact(setting);
    }
    std::thread tcpServerThread(tcpServer,&setting,bm);
    bm->Run();
    exit(1);
}
