# Terark DB BenchmarkTest

本程序执行各种 DB 的 Benchmark，支持的 DB 包括：

* RocksDB
* TerarkDB
* TerocksDB
* Wiredtiger


## 目录结构
```bash
build/Terark_Engine_Test # 编译出来的可执行文件
shell # shell 脚本
src
Schema
```

## 输入文件

- **数据源**: 必须是文本文件，每行一条记录，每条记录有多个字段，字段之间用分隔符分隔，分隔符默认是 '\t' ，分隔符可以在命令行指定
- **Key**: 也是文本文件，每行一个 key，该文件中的内容被放入一个数组，以便随机选取一个 key，来进行随机读相应的 value

测试程序对**数据源**的访问是顺序读，所以，当数据源比较大时，数据源可以是 gzip （或其他压缩形式），通过 shell 替换，可以在命令行将数据实时解压，传送给执行程序，例如:<br/>

```bash
diff <(zcat file1.gz) <(zcat file2.gz>
```
直接 diff file1.gz 和 file2.gz 解压后的内容，不需要先解压到中间文件。<br/>
当使用这个技巧时，zcat 可能会成为瓶颈，因为 gzip 的解压速度可能低于 测试程序 读取输入并插入 DB 的速度。

## RocksDB 存储格式

* Key: 不管字段分隔符是啥，单条包含多个字段的组合 key ，字段之间总是以空格分隔。
* Value: 输入数据中，去除掉 Key 以外的所有其它字段，按顺序拼接，不管字段分隔符是啥，统一使用 '\t' 做分隔。


## 命令行参数

命令行格式： `build/Terark_Engine_Test WhichDB Options`

WhichDB 可以是:
* rocksdb
* terarkdb
* terocksdb
* wiredtiger

目前经过充分测试的是 rocksdb 和 terocksdb 。

### 选项参数(Options)

必需有参数的选项名均以 '=' 结尾，例如 `--keyfields='|'`

|选项(Option)|说明|
|------------|---------------|
|--keyfields=   |逗号(,)分隔的数字列表，<br/>每个数字表示一个字段序号(字段序号从0开始)|
|--numfields=   |每条记录有多少个字段，仅用于数据合法性检查|
|--fields\_delim= |字段分隔符，不指定该参数时，默认'\t'，TPC-H数据的分隔符是'&#124;'，<br/>shell脚本中需要将'&#124;'放入引号中，否则'&#124;' 会被解释为管道|
|--insert\_data\_path=|**数据源**的文件名，可以是 shell 替换，例如 `<(zcat data.gz)`|
|--keys\_data\_path=|预抽取出来的 key 文件，用来进行随机读|
|--logdir=|用于自定义 RocksDB 的 logdir(记录状态和出错信息)|
|--waldir=|用于自定义 RocksDB 的 waldir(wal 指 Write Ahead Log)|
|--db=|数据库目录，在 RocksDB 中，可以指定多个数据库目录，此时，<br/>参数的格式为: `size1:dir1,size2:dir2,...`，<br/>size表示这个目录可用的空间配额，可以加K,M,G,T后缀，例如100G<br/>rocksdb在多个目录之间的容量平衡分配有问题，有些情况下可能达不到预期效果|
|--alt\_engine\_name=|为这次测试指定一个名字/标签|
|--disable\_wal|禁用 Write Ahead Log, 这会提高写性能，但出错时会丢失数据|
|--flush\_threads=|RocksDB 的 Flush 线程数（将 MemTable 刷新到 SST 文件的线程数），<br/>(Flush 线程的优先级高于 Compact)|
|--num\_levels=|RocksDB 的 Level 数量|
|--target\_file\_size\_multiplier=|层数每增加一层，单个 SST 文件的尺寸增加到这么多倍|
|--universal\_compaction=|1或0，1表示使用univeral compaction，0表示使用 Level based compaction|
|--auto\_slowdown\_write=|1或0，为1时，可能会因为 compact 太慢，导致写降速，<br/>为 0 时，对写速度不做限制，总是尽最大速度写入|
|--thread\_num=|前台线程数（对数据库执行读/写/更新操作的线程），<br/>线程编号从0开始，对应前闭后开区间 [0, n) |
|--plan\_config=|参数格式 `configId:读百分比:写百分比:更新百分比`，<br/>和 `--thread_plan_map` 配合，可以<br/>让不同的线程按预定义的读/写/更新比例执行|
|--thread\_plan\_map=|参数格式 `线程编号范围:configId`，<br/>线程编号范围格式 `min-max`，指闭区间[min,max]，<br/>线程编号范围也可以是单个线程编号|
|--terocksdb\_tmpdir=|terocksdb 专用的临时目录，测试 terocksdb 时必须指定|
|--mysql\_passwd=|指定监控数据库(MySQL)的密码|

