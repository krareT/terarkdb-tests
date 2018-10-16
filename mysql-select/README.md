# mysql-select

用于测试 select * from tables 速度的小工具

测试数据库：mysql-5.6.35（InnoDB）
select语句：select * from lineitem

## 测试结果

### 1. 客户端与服务端不同机器同内网

结果：

```
Fetching results ...
total rows fetched: 100000000
total length of data: 19297617734
average length of data: 192

real	2m53.196s
user	0m24.683s
sys	0m10.583s
```

速度：**106.26 MB/s**

mysqld 内存占用： **38.91G**，cpu 占用：**100%**

### 2. 客户端与服务端在同一台机器

结果：

```
total rows fetched: 100000000
total length of data: 19297617734
average length of data: 192

real	3m1.411s
user	0m35.736s
sys	0m12.740s
```

速度：**101.45 MB/s**

mysqld 内存占用： **38.91G**，cpu 占用：**100%**

## 测试数据

### 1. 表结构

```
CREATE TABLE `lineitem` (
  `L_ORDERKEY` bigint(20) NOT NULL,
  `L_PARTKEY` bigint(20) NOT NULL,
  `L_SUPPKEY` int(11) NOT NULL,
  `L_LINENUMBER` int(11) NOT NULL,
  `L_QUANTITY` decimal(15,2) NOT NULL,
  `L_EXTENDEDPRICE` decimal(15,2) NOT NULL,
  `L_DISCOUNT` decimal(15,2) NOT NULL,
  `L_TAX` decimal(15,2) NOT NULL,
  `L_RETURNFLAG` char(1) COLLATE utf8_bin NOT NULL,
  `L_LINESTATUS` char(1) COLLATE utf8_bin NOT NULL,
  `L_SHIPDATE` date NOT NULL,
  `L_COMMITDATE` date NOT NULL,
  `L_RECEIPTDATE` date NOT NULL,
  `L_SHIPINSTRUCT` char(25) COLLATE utf8_bin NOT NULL,
  `L_SHIPMODE` char(10) COLLATE utf8_bin NOT NULL,
  `L_COMMENT` varchar(512) COLLATE utf8_bin NOT NULL
) ENGINE=InnoDB;
```

### 2. 源数据

tpch-dbgen 生成数据

```
env L_CMNT_LEN=10 ./dbgen -T L -s 12
```

总条数： **100,000,000**

数据示例：

```
1|7759468|384484|1|17|25960.36|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|above the|
1|3365454|365455|2|36|54694.44|0.09|0.06|N|O|1996-04-12|1996-02-28|1996-04-20|TAKE BACK RETURN|MAIL|es: slyly bol|
1|3184989|184990|3|8|16590.64|0.10|0.02|N|O|1996-01-29|1996-03-05|1996-01-31|TAKE BACK RETURN|REG AIR|regular, ex|
1|106575|231576|4|28|44283.96|0.09|0.06|N|O|1996-04-21|1996-03-30|1996-05-16|NONE|AIR| fluffily|
1|1201332|76339|5|24|29598.48|0.10|0.04|N|O|1996-03-30|1996-03-14|1996-04-01|NONE|FOB|s. slyly |
1|781723|31726|6|32|57750.08|0.07|0.02|N|O|1996-01-30|1996-02-07|1996-02-03|DELIVER IN PERSON|MAIL|ly sly|
2|5308487|58508|1|38|56818.36|0.00|0.05|N|O|1997-01-28|1997-01-14|1997-02-02|TAKE BACK RETURN|RAIL|osits breach|
3|214849|89850|1|45|79372.35|0.06|0.00|R|F|1994-02-02|1994-01-04|1994-02-23|NONE|AIR|riously brave|
3|951772|326776|2|49|89362.77|0.10|0.00|R|F|1993-11-09|1993-12-20|1993-11-24|TAKE BACK RETURN|RAIL|unusual |
3|6422412|172437|3|27|36020.43|0.06|0.07|A|F|1994-01-16|1993-11-22|1994-01-23|DELIVER IN PERSON|SHIP|counts|
```
