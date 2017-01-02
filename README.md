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
当使用这个技巧时，zcat 可能会成为瓶颈，因为 gzip 的解压速度可能低于 测试程序 读取输入并插入 DB 的速度；并且，这种实时解压会占用一个 CPU，所以，只有在磁盘空间不足时，再使用这种方法。

## RocksDB 存储格式

* Key: 单个 Key 可以是包含多个字段的组合 Key ，组合 Key 的多个字段之间用记录分隔符分隔。
* Value: 输入数据中，去除掉 Key 以外的所有其它字段，按顺序拼接，并使用记录分隔符分隔。

### 分离 Key Value
我们提供了一个程序(`splitkv`)来将文本数据库（每行由分隔符分隔成字段）中的 key value 拆分成分别的 key文件 和 value文件（用 cut 或 awk 也可以，但速度太慢）。

分离的 Key 与 Value 在文本文件中的格式与在 RocksDB 中存储的格式完全相同。

```bash
cat  lineitem.tbl    | splitkv.exe -k 0,1,2 -d'|' lineitem.key lineitem.value
# 或者，如果使用了 dbgen.sh 进行了压缩：
zcat lineitem.tbl.gz | splitkv.exe -k 0,1,2 -d'|' lineitem.key lineitem.value
```

分离的 key 可以使用 [nlt\_build](https://github.com/Terark/terark-wiki-zh_cn/blob/master/tools/bin/nlt_build.exe.md) 进行压缩并测试<br/>
分离的 value 可以使用 [zbs\_build](https://github.com/Terark/terark-wiki-zh_cn/blob/master/tools/bin/zbs_build.exe.md) 进行压缩并测试

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
|--action=|load 或者 run，目前仅使用 run|
|--key\_fields=   |逗号(,)分隔的数字列表，<br/>每个数字表示一个字段序号(字段序号从0开始)|
|--fields\_num=   |每条记录有多少个字段，仅用于数据合法性检查|
|--fields\_delim= |字段分隔符，不指定该参数时，默认'\t'，TPC-H数据的分隔符是'&#124;'，<br/>shell脚本中需要将'&#124;'放入引号中，否则'&#124;' 会被解释为管道|
|--insert\_data\_path=|**数据源**的文件名，可以是 shell 替换，例如 `<(zcat data.gz)`|
|--keys\_data\_path=|预抽取出来的 key 文件，用来进行随机读|
|--cache\_size=|RocksDB/wiredtiger 数据库缓存的尺寸，可以使用 K,M,G,T 后缀。<br/>注意：操作系统的 pagecache 需要另外的内存，如果 cache\_size 设置过大，<br/>可能会导致操作系统 pagecache 太小不够用而引起一些问题|
|--logdir=|用于自定义 RocksDB 的 logdir(记录状态和出错信息)|
|--waldir=|用于自定义 RocksDB 的 waldir(wal 指 Write Ahead Log)|
|--db=|数据库目录，在 RocksDB 中，可以指定多个数据库目录，此时，<br/>参数的格式为: `size1:dir1,size2:dir2,...`，<br/>size表示这个目录可用的空间配额，可以加K,M,G,T后缀，例如100G<br/>rocksdb在多个目录之间的容量平衡分配有问题，有些情况下可能达不到预期效果|
|--alt\_engine\_name=|为这次测试指定一个名字/标签|
|--disable\_wal|禁用 Write Ahead Log, 这会提高写性能，但出错时会丢失数据|
|--flush\_threads=|RocksDB 的 Flush 线程数（将 MemTable 刷新到 SST 文件的线程数），<br/>(Flush 线程的优先级高于 Compact)|
|--num\_levels=|RocksDB 的 Level 数量|
|--write\_rate\_limit=|设定写速度，尽量按此速度进行写入，默认 30MB/s<br/>当此参数 **非0** 时，auto\_slowdown\_write参数失效<br/>当此参数 **为0** 时，auto\_slowdown\_write参数**生效**|
|--target\_file\_size\_multiplier=|层数每增加一层，单个 SST 文件的尺寸增加到这么多倍|
|--universal\_compaction=|1或0，1表示使用univeral compaction，0表示使用 Level based compaction|
|--auto\_slowdown\_write=|1或0，为1时，可能会因为 compact 太慢，导致写降速，<br/>为 0 时，对写速度不做限制，总是尽最大速度写入<br/>仅当 write\_rate\_limit 参数为0时，此参数才生效|
|--thread\_num=|前台线程数（对数据库执行读/写/更新操作的线程），<br/>线程编号从0开始，对应前闭后开区间 [0, n) |
|--plan\_config=|参数格式 `configId:读百分比:写百分比:更新百分比`，<br/>和 `--thread_plan_map` 配合，可以<br/>让不同的线程按预定义的读/写/更新比例执行|
|--thread\_plan\_map=|参数格式 `线程编号范围:configId`，<br/>线程编号范围格式 `min-max`，指闭区间[min,max]，<br/>线程编号范围也可以是单个线程编号<br/>未指定 thread\_plan\_map 时，每个线程的默认 configId 是 0|
|--terocksdb\_tmpdir=|terocksdb 专用的临时目录，测试 terocksdb 时必须指定|
|--mysql\_passwd=|指定监控数据库(MySQL)的密码|


### 命令行样例

`shell` 目录中有一些脚本，是命令行使用的标准样例，例如 [Terocksdb\_Tpch\_Run.sh](shell/Terocksdb\_Tpch\_Run.sh)


### 环境变量
有一些全局参数是通过环境变量控制的：

|环境变量名|说明|
|----------|----|
|DictZipBlobStore\_zipThreads|该变量未设置时(默认)，相当于 8 ，<br/>如果机器的 CPU 数量小于 8 ，就是实际的 CPU 数量；<br/>如果设为 0，表示不使用多线程压缩；<br/>非0时，总是使用多线程压缩，读、压缩、写线程是分离的，<br/>默认值可以工作的不错，如果需要，可以进行精细控制，<br/>对于TPC-H数据，8个线程的压缩速度可以达到200MB/s|

## TPC-H 测试数据
我们对 TPC-H 的 dbgen 做了一些修改，改变文本字段的长度，用来生成我们需要的数据。

TPC-H 的多个表中， lineitem 表尺寸最大，所以我们使用 lineitem 表的数据进行测试。**注意**: TPC-H dbgen 生成的数据库文本文件，记录的分隔符是 '|' 。

TPC-H lineitem 表有个字段 comment，是文本类型，该字段贡献了大部分压缩率，dbgen 中该字段的尺寸是硬编码为 44 个字节。为了符合测试要求，我们需要修改该字段的长度。我们基于 tpch\_2\_17 做了[这个修改](https://github.com/rockeet/tpch-dbgen/commit/13bf6a246514bb500ff0ab4991b36735110e3f8f)。

我们增加了一个新的脚本 dbgen.sh 用于直接压缩生成的数据库表，请参考: [github链接](https://github.com/rockeet/tpch-dbgen)

## 预先生成随机采样的 Query Key
通过分离 key value，我们可以得到所有的 key，为了测试随机读性能，我们需要把所有的 Key 都放入内存，并且支持在常数时间内随机取一个 Key，要实现这个需求，最简单的做法是使用 std::vector&lt;std::string&gt;，但这样消耗的内存太多，我们使用了一种简单的优化存储 [fstrvec](https://github.com/Terark/terark-db/tree/master/terark-base/src/terark/util/fstrvec.hpp)，然而，但即使这样，把全部的 Key 保存在内存中，在很多情况下也不太现实(TPC-H 短数据lineitem.comment=512字节时，550GB 数据中 Key 占 22GB)。

所以，我们只在内存中存储一部分 Key，为了达到“随机”读的效果，这些 Key 必须是随机选取的，可以使用 `awk` 脚本，来完成这个随机选取的功能：
```awk
awk '{if(rand()<0.07)print($0)}' # 随机选取 7% 的 key
```

得到的这个采样 key 文件(sampled.keys.txt)之后，将参数 `--keys_data=sampled.keys.txt` 传给测试程序

