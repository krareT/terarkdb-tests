[中文](https://github.com/Terark/terarkdb-tests/blob/master/docs/首页.md)

# Terark DB BenchmarkTest

This program can benchmark on different kinds of DB:

* RocksDB  (RocksDB official version)
* TerarkDB (RocksDB on Terark searchable compression algorithms)
* TerichDB
* WiredTiger


## Directory

```bash
build/Terark_Engine_Test # Executable binary
shell # shell scripts
src
Schema
```

## Input

- **Data Source**: Must be raw text, one record per line. Each of the line has multiple fields that splited by `\t`(The field spliter cold be specified by command line option).
- **Key**: Should also be raw text, each line is a key. These keys will be placed into an array that are used for pick up random keys during the reading.

This program read **Data Source** sequentially, so when you have a huge dataset you can pack the dataset by gzip(or other compressed formats) and then use shell commands to de-compress it (real-time de-compressing) and feed it into the program. For example :


```bash
diff <(zcat file1.gz) <(zcat file2.gz)
```

This will diff directly the content of `file1.gz` and `file2.gz` without de-compressed tmp files.

When using this trick, `zcat` may be a bottomneck since `gzip` may have a lower de-compress speed than the DB insert speed, and it will also uses a CPU during de-compressing. So please use this trick only when you have a limited disk resource.

## RocksDB Input Data Format

* Key: Each of the KEY could be a combination of multiple fields that separated with separators.
* Value: The rest of the fields combined with separators are used as Value.

### Split Key and Value
We provide a tool([splitkv for linux](http://nark.cc/download/splitkv.exe)) to help you split your data source (each line is a combination of multiple fields) into separated key file and value file.(`cut` or `awk` will also works, but too slow)

The separated Key file and Value file have the same format with the format of storage in RocksDB.

```bash
cat  lineitem.tbl    | splitkv.exe -k 0,1,2 -d'|' lineitem.key lineitem.value

# If you used `dbgen.sh` for compression
zcat lineitem.tbl.gz | splitkv.exe -k 0,1,2 -d'|' lineitem.key lineitem.value
```

The separated key file can be compressed and tested by [nlt\_build](https://github.com/Terark/terark-wiki-zh_cn/blob/master/tools/bin/nlt_build.exe.md)

The separated value file can be compressed and tested by [zbs\_build](https://github.com/Terark/terark-wiki-zh_cn/blob/master/tools/bin/zbs_build.exe.md)

## Command Line Options

Command Example： `build/Terark_Engine_Test WhichDB Options`

`WhichDB` could be one of these :
* rocksdb
* terarkdb
* terichdb
* wiredtiger

The fully tested engine includes : rocksdb, wiredtiger, terarkdb 。

### Options

The required options are followed by a `=`, e.g. `--keyfields='|'`

|Option|Description|
|------------|---------------|
|--action=|`load` or `run`<br/>`load` is for write only, write all data into DB ASAP and compact, the program will exit after the compaction.<br/>`run` will excute the read & write test, keeps running until user press `Ctrl + C`|
|--key\_fields=   |Numbers that are separated by a comma (,)<br/> Each number represent a field (start from 0)|
|--fields\_num=   |每条记录有多少个字段，仅用于数据合法性检查|
|--fields\_delim= |字段分隔符，不指定该参数时，默认'\t'，TPC-H数据的分隔符是'&#124;'，<br/>shell脚本中需要将'&#124;'放入引号中，否则'&#124;' 会被解释为管道|
|--insert\_data\_path=|**数据源**的文件名，可以是 shell 替换，例如 `<(zcat data.gz)`|
|--load\_data\_path=|**数据源**的文件名，可以是 shell 替换，例如 `<(zcat data.gz)`<br/>load 速度比较快，`zcat`的速度可能跟不上，所以，最好使用SSD上存储的未压缩的文件|
|--keys\_data\_path=|预抽取出来的 key 文件，用来进行随机读|
|--cache\_size=|RocksDB/wiredtiger 数据库缓存的尺寸，可以使用 K,M,G,T 后缀。<br/>注意：操作系统的 pagecache 需要另外的内存，如果 cache\_size 设置过大，<br/>可能会导致操作系统 pagecache 太小不够用而引起一些问题|
|--logdir=|用于自定义 RocksDB 的 logdir(记录状态和出错信息)|
|--waldir=|用于自定义 RocksDB 的 waldir(wal 指 Write Ahead Log)|
|--db=|数据库目录，在 RocksDB 中，可以指定多个数据库目录，此时，<br/>参数的格式为: `size1:dir1,size2:dir2,...`，<br/>size表示这个目录可用的空间配额，可以加K,M,G,T后缀，例如100G<br/>rocksdb在多个目录之间的容量平衡分配有问题，有些情况下可能达不到预期效果|
|--alt\_engine\_name=|为这次测试指定一个名字/标签|
|--disable\_wal|禁用 Write Ahead Log, 这会提高写性能，但出错时会丢失数据|
|--flush\_threads=|RocksDB 的 Flush 线程数（将 MemTable 刷新到 SST 文件的线程数），<br/>（Flush 线程的优先级高于 Compact）|
|--num\_levels=|RocksDB 的 Level 数量|
|--write\_buffer\_size=|默认 1G|
|--write\_rate\_limit=|设定写速度，尽量按此速度进行写入，默认 30MB/s<br/>当此参数 **非0** 时，auto\_slowdown\_write参数**失效**<br/>当此参数 **为 0** 时，auto\_slowdown\_write参数**生效**|
|--target\_file\_size\_multiplier=|层数每增加一层，单个 SST 文件的尺寸增加到这么多倍|
|--enable\_auto\_compact=|1 或 0，1 表示启用自动 compact，0 表示禁用自动 compact，默认为 1|
|--rocksdb\_memtable=|如果指定该参数，必须是 vector（使用 VectorRepFactory）<br/>不指定的话，使用 rocksdb 的默认 memtable<br/> vector memtable 仅在 --action=load 时有用，性能更好|
|--load\_size=|如果输入文件尺寸过大，指定该参数可以只加载这么多数据就停止写操作<br/>(`--action=run`的时候，停止写操作，读操作仍会继续)|
|--use\_universal\_compaction=|1 或 0，1 表示使用 univeral compaction，0 表示使用 Level based compaction，默认为 1|
|--auto\_slowdown\_write=|1 或 0，默认 1<br/>为 1 时，可能会因为 compact 太慢，导致写降速，<br/>为 0 时，对写速度不做限制，总是尽最大速度写入<br/>仅当 write\_rate\_limit 参数为 0 时，此参数才生效|
|--index\_nest\_level=|默认 3，最小为 2，对 TPC-H，设为 2 可以提高大约 10% 的读性能<br/>默认 3 是个比较均衡的值，更大的值有助于提高 index 的压缩率，但会降低性能|
|--zip\_work\_mem\_soft\_limit=|默认 16G，值越大，越有利于提高大尺寸 SST 的压缩速度(并发度会提高)|
|--zip\_work\_mem\_hard\_limit=|默认 32G，...|
|--small\_task\_mem=|默认 2G，内存用量小于该尺寸的压缩任务，会忽略 zip\_work\_mem\_soft\_limit, 直接执行<br/>当所有压缩线程的总内存用量达到 zip\_work\_mem\_hard\_limit 时，仍必须等待|
|--checksum\_level=|默认 1，仅检查 metadata ，<br/>设为 2 会在每条记录上增加一个 4 bytes 的 checksum，<br/>设为 3 时，SST 整体计算并验证 checksum|
|--terarkdb\_sample\_ratio=|默认 0.015，terark全局压缩的采样率|
|--terarkdb\_zip\_min\_level=|默认 0，只有 level 大于等于此值时，才使用 terark SST，<br/>否则使用 rocksdb 自身的 BlockBasedTable|
|--index\_cache\_ratio=|默认 0.002，可以提高精确查找(DB.Get)的性能(0.002可以提高大约10%)，<br/>该值设置得越大，对性能提升的帮助越小<br/>该设置对通过 iterator 进行查找/遍历无任何帮助|
|--thread\_num=|前台线程数（对数据库执行读/写/更新操作的线程），<br/>线程编号从0开始，对应前闭后开区间 [0, n) |
|--plan\_config=|参数格式 `configId:读百分比:写百分比:更新百分比`，<br/>和 `--thread_plan_map` 配合，可以<br/>让不同的线程按预定义的读/写/更新比例执行|
|--thread\_plan\_map=|参数格式 `线程编号范围:configId`，<br/>线程编号范围格式 `min-max`，指闭区间[min,max]，<br/>线程编号范围也可以是单个线程编号<br/>未指定 thread\_plan\_map 时，每个线程的默认 configId 是 0|
|--terarkdb\_tmpdir=|terarkdb 专用的临时目录，测试 terarkdb 时必须指定|
|--mysql\_passwd=|指定监控数据库(MySQL)的密码|


### Command Examples

The `shell` directory has some scripts that use standard commands, e.g. [Terarkdb\_Tpch\_Run.sh](shell/Terarkdb\_Tpch\_Run.sh)


### Environment Variables
Some global parameters are set by environment variables:

<table><tbody>
<tr><th>Variable</th><th>Description</th></tr>
<tr>
 <td>DictZipBlobStore_zipThreads</td>
 <td>该变量未设置时(默认)，相当于 8<br/>
如果机器的 CPU 数量小于 8 ，就是实际的 CPU 数量；<br/>
如果设为 0，表示不使用多线程压缩；</br>
非0时，总是使用多线程压缩，读、压缩、写线程是分离的，<br/>
默认值可以工作的不错，如果需要，可以进行精细控制，<br/>
对于TPC-H数据，8个线程的压缩速度可以达到200MB/s
 </td>
</tr>
<tr>
 <td>MYSQL_SERVER</td>
 <td>该变量未设置时(默认)，使用 Terark 的 Mysql Server </td>
</tr>
<tr>
 <td>MYSQL_PORT</td>
 <td>该变量未设置时(默认)，使用 Mysql 默认的端口 3306 </td>
</tr>
<tr>
 <td>MYSQL_USER</td>
 <td>该变量未设置时(默认)，使用 Terark 默认的 user</td>
</tr>
<tr>
 <td>MONITOR_STAT_FILE_PREFIX</td>
 <td>将系统资源监控数据写入文本文件，有多个文件，该变量指定这些文件名的前缀
  <br/>这些文本文件都是CSV格式（逗号分隔字段）
  <table><tbody>
  <tr>
   <td>${prefix}-ops.txt</td>
   <td>db 操作监控（读、写、更新数量），数据格式：<br/>
   time, op_num, op_type<br/>
   op_type: 0 表示读，1 表示写，2 表示更新  
   </td>
  </tr>
  <tr>
   <td>${prefix}-cpu.txt</td>
   <td>cpu 监控，数据格式：  time, cpu%, iowait</td>
  </tr>
  <tr>
   <td>${prefix}-memory.txt</td>
   <td>内存 监控，数据格式（单位是KB）：<br/>
    time, total, free, cached, resident
   </td>
  </tr>
  <tr>
   <td>${prefix}-dbsize.txt</td>
   <td>db尺寸，数据格式： time, dbsizeKB</td>
  </tr>
  <tr>
   <td>${prefix}-diskinfo.txt</td>
   <td>db尺寸详细信息，数据格式： time, description<br/>
   description 详细描述每个 db 目录的尺寸
   </td>
  </tr>
  </tbody></table>
 </td>
</tbody></table>


## TPC-H Dataset
We've changed `TPC-H`'s `dbgen` a little to generate the data we need, mostly changed its field length.

In the tables of `TPC-H`, `lineitem` has the largest table size, so we use `lineitem` for testing. **Notice**: TPC-H dbgen uses `|` as record separator.

TPC-H lineitem has a `comment` field, which is raw text format. This field conttribute the most to the compression. The field in `dbgen` is hard coded as 27 bytes. To make it flexiable and fit our needs, we added a feature that let us change the field length via environemnt variable. And besides that we added a new `dbgen.sh` to generate compressed db tables directly. [dbgen.sh intro](https://github.com/rockeet/tpch-dbgen)

## Preloaded Key for Random Read
By separating Keys and Values, we now have all Keys. For the convinient of random reading test, we need to load all Keys into memory and make it possible to pick a random Key in constant time. The easiest way to make it is using `std::vector<std::string>`, but it costs too much memory, so we use an optimized way : [fstrvec](https://github.com/Terark/terichdb/tree/master/terark-base/src/terark/util/fstrvec.hpp).

Even so, it is not a good practice to load all Keys into memory(e.g. For `TPC-H lineitem.comment=512 bytes`，in a 550GB dataset, Keys will cost 22GB)。

So, we only pick part of the Keys for random reading, these Keys must be randomly picked. e.g. by `awk` :

```awk
awk '{if(rand()<0.07)print($0)}' # randomly pick 7% of all keys
```

You can use the option `--keys_data=sampled.keys.txt` to pass the Key file (sampled.keys.txt) to the test program.
