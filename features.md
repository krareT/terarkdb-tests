## 随机读 测试 + 正确性验证

### 为该功能单独增加一个命令行选项

选项： `--verify_kv_file=/path/to/shuf_file`<br/>
该选项指定乱序后的输入文件

### 生成输入数据

对 data input file 做一个随机乱序，例如：

```
shuf movies_flat.txt > movies_flat_shuf.txt
```
### 按 key 搜索并验证结果

顺序读 shuf 后的文件，每次读一行，然后从中抽取出 key 和 value：
```
  if (setting.splitKeyValue(line, &key, &value)) {
    // do search
    if (db.Get(key, &value2)) {
      // compare value and value2 to verify correctness
    } else {
      // error, not found
    }
  }
```

因为总是顺序读一行，所以输入文件可以是管道，例如，只要程序不结束，就一直测试：
```
cmd --verify_kv_file=<(while true; do cat shuf_file; done)
```

