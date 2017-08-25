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
|--fields\_num=   |How many fields each record, only use for data validation|
|--fields\_delim= |Fields delimator, default '\t'，TPC-H's delimator is '&#124;'，<br/>when using under shell script, '&#124;' shoule be placed between '', or it will be treated as shell pipeline|
|--insert\_data\_path=|**data source** filename，could use shell as replacement, e.g. `<(zcat data.gz)`|
|--load\_data\_path=|**data source** filename，could use shell as replacement, e.g. `<(zcat data.gz)`<br/>since load is very fast，`zcat` may couldn't follow, so its better to use uncompressed files in SSD|
|--keys\_data\_path=|pre-loaded key files，using for random read|
|--cache\_size=|RocksDB/wiredtiger db cache，could use `K,M,G,T` as suffix.<br/>Note：OS page cache also need memory, if cache_size is too large it may cause pronlems|
|--logdir=| RocksDB log dir|
|--waldir=| RocksDB WAL dir(wal : Write Ahead Log)|
|--db=|databse working dir, RocksDB can use multple working dirs，<br/>example: `size1:dir1,size2:dir2,...`，<br/>`size` represent the capacity of current dir, could use `K,M,G,T` as suffix, e.e. 100G<br/>RocksDB has some problem when balancing multple dirs, which may cause unexpected low performance.|
|--alt\_engine\_name=| Indicate current test label, can find it at `test.terark.com`|
|--disable\_wal|Disable Write Ahead Log. Can improve write performance but may lose data|
|--flush\_threads=|RocksDB' Flush thread count（wihch flush MemTable into SST file），<br/>（Flush threads has higher priority than Compact threads）|
|--num\_levels=|RocksDB's Level number|
|--write\_buffer\_size=|default 1G|
|--write\_rate\_limit=|Write speed limit, default 30MB/s<br/>When this param is **NOT 0**，auto\_slowdown\_write will be **ignored**<br/>When it **IS 0**，auto\_slowdown\_write **will be used**|
|--target\_file\_size\_multiplier=| New layer SST file size will be `target\_file\_size\_multiplier` times larger|
|--enable\_auto\_compact=|1 or 0，1 means auto compact，0 will disable compact，default is 1|
|--rocksdb\_memtable=|If this is set, then it must be vector（use VectorRepFactory）<br/>If not specified, use rocksdb's default memtable<br/> vector memtable will only useful when `--action=load` and has better performance|
|--load\_size=|If input file size is too large, the process will only read as much as the size data<br/>(when using `--action=run` the process will continue running but only stop writing)|
|--use\_universal\_compaction=|1 or 0，1 means univeral compaction，0 means Level based compaction，default is 1|
|--auto\_slowdown\_write=|1 or 0，default is 1<br/>When it's 1, compact will be too slow that cause a slower write speed.<br/>When is's 0 and without write limit，it will always try its best to write<br/>Only when write\_rate\_limit is 0，this param will be used|
|--index\_nest\_level=|Default is 3 and minimal is 2. For TPC-H，set this to 2 can improve about 10% read performance<br/>Default 3 is a balanced value，larger value can increase compression ratio of index，but drop performance|
|--zip\_work\_mem\_soft\_limit=|Default is 16G，larger value，faster SST compression speed(better concurrent performance)|
|--zip\_work\_mem\_hard\_limit=|Default is 32G，...|
|--small\_task\_mem=|Defaul is 2G. Tasks that need less memory will ignore `zip\_work\_mem\_soft\_limit` and execute directly.<br/>When the total memory usage execeeds `zip\_work\_mem\_hard\_limit`, Still need to wait|
|--checksum\_level=|Default is 1，only check metadata ，<br/>If set to 2 will add an extra 4 bytes checksum，<br/>When set to 3 ，will check checksum of the whole SST file|
|--terarkdb\_sample\_ratio=|Default is 0.015, terark global compression's sampling ratio|
|--terarkdb\_zip\_min\_level=|Defaul is 0. Only when level higher than this value, Terark SST will be enabled，<br/> Otherwise will still use rocksdb's BlockBasedTable|
|--index\_cache\_ratio=|Default is 0.002，can improve exact search(`DB.Get`) performance(0.002 for about 10%)，<br/> The higher of this value, the performacne improvement effect will be smaller<br/>This param does nothing with `iterator`|
|--thread\_num=| Frontend threads number（Total threads of read/write/update），<br/>Thread label will start at 0 |
|--plan\_config=|Format: `configId:read percent:write percent:update percent`，<br/>use this with `--thread_plan_map`，can make every thread works as predefined read/write/update percentage.|
|--thread\_plan\_map=|Format: `thread_range:configId`，<br/>thread_range example : `[min,max]`<br/>Can use a single thread.<br/>If no `thread\_plan\_map`, every thread will use the configId `0`|
|--terarkdb\_tmpdir=|terarkdb temp dir，must be set when testing `terarkdb`|
|--mysql\_passwd=| Specified MySQL password of the online monitor(`test.terark.com`)|


### Command Examples

The `shell` directory has some scripts that use standard commands, e.g. [Terarkdb\_Tpch\_Run.sh](shell/Terarkdb\_Tpch\_Run.sh)


### Environment Variables
Some global parameters are set by environment variables:

<table><tbody>
<tr><th>Variable</th><th>Description</th></tr>
<tr>
 <td>DictZipBlobStore_zipThreads</td>
 <td>When not set(default)，using 8<br/>
If CPU number less than 8，using CPU number；<br/>
If set to 0 means do no use multiple thread compression；</br>
If not 0，always use multiple threads. read/compress/write threads are seperated<br/>
The default value works fine, you can have more gradular control if you need，<br/>
For PC-H，8 threads can archieve 200MB/s speed
 </td>
</tr>
<tr>
 <td>MYSQL_SERVER</td>
 <td>If not set, use Terark's MySQL server</td>
</tr>
<tr>
 <td>MYSQL_PORT</td>
 <td>If not set, use 3306</td>
</tr>
<tr>
 <td>MYSQL_USER</td>
 <td>If not set, user default user</td>
</tr>
<tr>
 <td>MONITOR_STAT_FILE_PREFIX</td>
 <td>Write system monitor data into text file，the indicates the prefix of these files
  <br/>All of them are CSV format with `,` as delimator.
  <table><tbody>
  <tr>
   <td>${prefix}-ops.txt</td>
   <td>db operation count, (read, write, update)，format：<br/>
   time, op_num, op_type<br/>
   op_type: 0 (read)，1 (write)，2 (update)  
   </td>
  </tr>
  <tr>
   <td>${prefix}-cpu.txt</td>
   <td>cpu monitor，format：  time, cpu%, iowait</td>
  </tr>
  <tr>
   <td>${prefix}-memory.txt</td>
   <td>Memory monitor，format（KB）：<br/>
    time, total, free, cached, resident
   </td>
  </tr>
  <tr>
   <td>${prefix}-dbsize.txt</td>
   <td>db size，format： time, dbsizeKB</td>
  </tr>
  <tr>
   <td>${prefix}-diskinfo.txt</td>
   <td>db size detail, format： time, description<br/>
    `description` means every db dir's exact size
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
