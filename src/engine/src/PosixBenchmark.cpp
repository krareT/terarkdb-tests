//
// Created by terark on 9/10/16.
//

#include <dirent.h>
#include "PosixBenchmark.h"
#include <terark/util/autoclose.hpp>
#include <terark/io/FileStream.hpp>

using terark::FileStream;

/**
 * 构造函数
 * 调用基类的构造函数来构造
 */
PosixBenchmark::PosixBenchmark(Setting& setting):Benchmark(setting){
}

/**
 * 打开
 * 首先打开setting中需要使用的db
 * 然后关闭（雾
 */
void PosixBenchmark::Open(void) {
    DIR *dir;
    dir = opendir(setting.FLAGS_db.c_str());
    if (dir == NULL){
        throw std::invalid_argument(strerror(errno));
    }
    auto ret = closedir(dir);
    if ( ret != 0){
        throw std::invalid_argument(strerror(errno));
    }
}

/**
 * 关闭
 * 空函数...
 */
void PosixBenchmark::Close(void) {
}

/**
 * 加载
 * 将setting的loadDataPath复制到db的目录下
 */
void PosixBenchmark::Load(void) {
    DIR *dir;
    fprintf(stderr, "PosixBenchmakr::Load to : %s\n", setting.getLoadDataPath().c_str());
    dir = opendir(setting.getLoadDataPath().c_str());
    if (dir == NULL){
        throw std::invalid_argument(strerror(errno) + setting.getLoadDataPath());
    }
    int ret = closedir(dir);
    if (ret != 0){
        throw std::invalid_argument(strerror(errno) + setting.getLoadDataPath());
    }
    if (system(NULL) == 0)
        throw std::logic_error("system error:no bash to use!\n");

    std::string cp_cmd = "cp " + setting.getLoadDataPath() + " " + setting.FLAGS_db + "/" + " -r";
    fprintf(stderr, "%s\n", cp_cmd.c_str());
    ret = system(cp_cmd.c_str());
    if (ret == -1)
        throw std::runtime_error("cp_cmd execute failed,child ,can't create child process.\n");
    if (ret == 127)
        throw std::runtime_error("cp_cmd execute failed!\n");
    return ;
}

/**
 * 读取一个key
 * 首先获取随机key
 * 然后从数据库中读取这个key所代表的文件
 * 最后读取到buf指针指向的空间中
 * @param ts
 * @return
 */
bool PosixBenchmark::ReadOneKey(ThreadState *ts) {
    if (!getRandomKey(ts->key, ts->randGenerator))
        return false;
    std::string read_path = setting.FLAGS_db;
    read_path = read_path + "/" + ts->key;
    FileStream read_file(read_path, "r");
    size_t size = read_file.fsize();
    std::unique_ptr<char> buf(new char [size]);
    if (size != read_file.read(buf.get(), size)){
        fprintf(stderr,"posix read error:%s\n",strerror(errno));
        return false;
    }
    return true;
}

/**
 * 更新一个key
 * 首先随机获取一个key
 * 然后获取更新文件目录
 * 然后先读取更新文件，再写入
 * @param ts
 * @return
 */
bool PosixBenchmark::UpdateOneKey(ThreadState *ts) {
    if (!getRandomKey(ts->key, ts->randGenerator)){
        fprintf(stderr,"RocksDbBenchmark::UpdateOneKey:getRandomKey false\n");
        return false;
    }
    ts->key = setting.FLAGS_db + ts->key;
    std::unique_ptr<FILE, decltype(&fclose)> update_file(fopen(ts->key.c_str(),"r"),fclose);
    size_t size = FileStream::fpsize(update_file.get());
    std::unique_ptr<char> buf(new char[size]);
    if (size != fread(buf.get(),1,size,update_file.get())){
        fprintf(stderr,"posix update read error:%s\n",strerror(errno));
        return false;
    }
    if (size != fwrite(buf.get(),1,size,update_file.get())){
        fprintf(stderr,"posix update write error:%s\n",strerror(errno));
        return false;
    }
    return true;
}

/**
 * 插入一个key
 * 首先随机获取一个要插入的key
 * 然后得到setting里面的insertDataPath和数据库的路径
 * 最后先读出insertDataPath中的数据，再写入数据库
 * @param ts
 * @return
 */
bool PosixBenchmark::InsertOneKey(ThreadState *ts) {
    if (!updateDataCq.try_pop(ts->key)) {
        return false;
    }
    ts->str = setting.getInsertDataPath() + "/" + ts->key;
    std::unique_ptr<FILE, decltype(&fclose)> insert_source_file(fopen(ts->str.c_str(),"r"),fclose);
    ts->str = setting.FLAGS_db;
    ts->str = ts->str + "/" + ts->key;
    std::unique_ptr<FILE, decltype(&fclose)> insert_target_file(fopen(ts->str.c_str(),"w+"),fclose);
    size_t size = FileStream::fpsize(insert_source_file.get());
    std::unique_ptr<char> buf(new char[size]);
    if (size != fread(buf.get(),1,size,insert_source_file.get())){
        fprintf(stderr,"posix insert read error:%s\n",strerror(errno));
        return false;
    }
    if (size != fwrite(buf.get(),1,size,insert_target_file.get())){
        fprintf(stderr,"posix insert write error:%s\n",strerror(errno));
        return false;
    }
    return true;
}

/**
 * 验证
 * @return
 */
bool PosixBenchmark::VerifyOneKey(ThreadState *) {
    return false;
}

/**
 * 压缩
 * 直接返回false
 * @return
 */
bool PosixBenchmark::Compact(void) {
    return false;
}

/**
 * 创建一个ThreadState对象
 * @param whichSamplingPlan
 * @return
 */
ThreadState *PosixBenchmark::newThreadState(const std::atomic<std::vector<bool>*>* whichSamplingPlan) {
    return new ThreadState(threads.size(), whichSamplingPlan);
}



