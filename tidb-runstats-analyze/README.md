# 统计信息搜集

requirements:
- tidb.version >= 6.1.0
- python3 (>=3.7)


策略：
- 在mysql.analyze_jobs中一直未成功搜集的表（或分区）需重新搜集
- 健康度低于90的表(或者分区)需重新搜集
- 从来没搜集过统计信息的表(或者分区)需搜集
- 做过drop stats的表需要重新搜集
- 对于分区表，如果只是部分分区失败则只搜集失败的分区，否则搜集整个表
- 排除blob、clob、lob、text、midieum字段类型（这些字段不做统计信息搜集）
- 按照table_rows升序搜集
- 待统计信息表如果在最近慢日志中出现过，则优先搜集（优先级大于table_rows）
- 规定统计信息搜集时间窗口

扩展功能：
- 优先搜集慢查询中的表


使用方法：
```
PS E:\pythonProject\tidb_analyze\pythonProject> python .\tidb_analyze.py -h
usage: tidb_analyze.py [-h] [-H HOST] [-P PORT] [-u USER] [-p [PASSWORD]] [-d DATABASE] [-t TIMEOUT] [--preview]

analyze slow log

options:
  -h, --help            show this help message and exit
  -H HOST, --host HOST  database host (default: 127.0.0.1)
  -P PORT, --port PORT  database port (default: 4000)
  -u USER, --user USER  database user (default: root)
  -p [PASSWORD], --password [PASSWORD]
                        database password (default: None)
  -d DATABASE, --database DATABASE
                        database name (default: information_schema)
  -t TIMEOUT, --timeout TIMEOUT
                        timeout (default: 43200)
  --preview             开启预览模式，不搜集统计信息搜集 (default: False)
```

```
example1:
PS E:\pythonProject\tidb_analyze\pythonProject> python .\tidb_analyze.py -H 192.168.31.201 -P 4000 -u root -p 
password:
2023-12-30 21:44:15,056 - INFO - 统计信息搜集失败的对象数为: 0
2023-12-30 21:44:16,328 - INFO - 从来没搜集过统计信息的表(不包含分区)数为: 0
2023-12-30 21:44:16,587 - INFO - 需要做统计信息搜集的对象数为: 0

example2:
PS E:\pythonProject\tidb_analyze\pythonProject> python .\tidb_analyze.py -H 192.168.31.201 -P 4000 -u root -p --start-time="00:00"
password:
2023-12-30 22:00:24,214 - INFO - 统计信息搜集失败的对象数为: 0
2023-12-30 22:00:25,560 - INFO - 从来没搜集过统计信息的表(不包含分区)数为: 1
2023-12-30 22:00:25,740 - INFO - 需要做统计信息搜集的对象数为: 1
2023-12-30 22:00:25,743 - WARNING - 当前时间:2023-12-30 22:00:25.742658，不在指定时间范围内[00:00-06:00]，不执行统计信息搜集: analyze table `test1`.`d10`，表记录数: test1.d10 = 0，后面表均不执行

example3:
PS E:\pythonProject\tidb_analyze\pythonProject> python .\tidb_analyze.py -H 192.168.31.201 -P 4000 -u root -p --preview
password:
2023-12-30 22:16:19,182 - INFO - 统计信息搜集失败的对象数为: 0
2023-12-30 22:16:20,548 - INFO - 从来没搜集过统计信息的表(不包含分区)数为: 1
2023-12-30 22:16:20,726 - INFO - 需要做统计信息搜集的对象数为: 1
2023-12-30 22:16:20,726 - INFO - 预览: analyze table `test1`.`d12`，搜集前表记录数: test1.d12 = 0

example4:
PS E:\pythonProject\tidb_analyze\pythonProject> python .\tidb_analyze.py -H 192.168.31.201 -P 4000 -u root -p          
password:
2023-12-30 22:16:44,574 - INFO - 统计信息搜集失败的对象数为: 0
2023-12-30 22:16:45,944 - INFO - 从来没搜集过统计信息的表(不包含分区)数为: 1
2023-12-30 22:16:46,123 - INFO - 需要做统计信息搜集的对象数为: 1
2023-12-30 22:16:46,157 - INFO - 执行: analyze table `test1`.`d12`，搜集前表记录数: test1.d12 = 0，耗时: 0.03秒

```