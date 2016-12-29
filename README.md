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

测试程序对**数据源**的访问是顺序读，所以，当数据源比较大时，数据源可以是 gzip （或其他压缩形式），通过 shell 替换，可以在命令行将数据实时解压，传送给压缩程序，例如:<br/>

```bash
diff <(zcat file1.gz) <(zcat file2.gz>
```
直接 diff file1.gz 和 file2.gz 解压后的内容，不需要先解压到中间文件。<br/>
当使用这个技巧时，zcat 可能会成为瓶颈，因为 gzip 的解压速度可能低于 测试程序 读取输入并插入 DB 的速度。


## 命令行参数

命令行格式： `build/Terark_Engine_Test WhichDB Options`

|选项(Option)|是否有<br/>参数|参数<br/>默认值|说明|
|------------|---------------|---------------|----|
|--keyfields   |Yes||逗号(,)分隔的数字列表，<br/>每个数字表示一个字段序号(字段序号从0开始)|
|--numfields   |Yes||每条记录有多少个字段，仅用于数据合法性检查|
|--fieldsDelim |Yes|'\t'|字段分隔符，TPC-H数据的分隔符是'&#124;'，shell脚本中需要将'&#124;'放入引号中，否则'&#124;' 会被解释为管道|
