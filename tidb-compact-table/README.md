# 针对表做Compaction操作

requirements:
- python3 (>=3.7) or python2 (>=2.7)
- 在tiup中控机上用安装用户执行

## 使用方法
1. 对一个两张表做Compaction操作
```text
(env) [tidb@host0 prometheus]$ python main.py -c tidb-test -t tpch.customer,tpch.orders
2024-07-25 15:59:47,388 - root-main.py[line:434] - INFO - get table:tpch.customer region info:http://192.168.31.201:10080/tables/tpch/customer/regions
2024-07-25 15:59:47,389 - root-main.py[line:506] - INFO - dbname:tpch,tabname:customer data_region_count:1,index_region_count:0,table_region_count:1
2024-07-25 15:59:47,389 - root-main.py[line:954] - INFO - table:tpch.customer,total region count:1
2024-07-25 15:59:47,389 - root-main.py[line:965] - INFO - start compact table:tpch.customer,total region count:1, current region num:1,region_id:127,peer_id:128
2024-07-25 15:59:47,434 - root-main.py[line:941] - INFO - cmd:tiup ctl:v8.0.0 tikv --host 192.168.31.201:20160 compact -r 127 -c default -d kv --bottommost force --threads 4,message: compact sucess!
2024-07-25 15:59:48,000 - root-main.py[line:941] - INFO - cmd:tiup ctl:v8.0.0 tikv --host 192.168.31.201:20160 compact -r 127 -c write -d kv --bottommost force --threads 4,message: compact sucess!
2024-07-25 15:59:48,000 - root-main.py[line:434] - INFO - get table:tpch.orders region info:http://192.168.31.201:10080/tables/tpch/orders/regions
2024-07-25 15:59:48,001 - root-main.py[line:506] - INFO - dbname:tpch,tabname:orders data_region_count:3,index_region_count:0,table_region_count:3
2024-07-25 15:59:48,002 - root-main.py[line:954] - INFO - table:tpch.orders,total region count:3
2024-07-25 15:59:48,002 - root-main.py[line:965] - INFO - start compact table:tpch.orders,total region count:3, current region num:1,region_id:147,peer_id:148
2024-07-25 15:59:48,048 - root-main.py[line:941] - INFO - cmd:tiup ctl:v8.0.0 tikv --host 192.168.31.201:20160 compact -r 147 -c default -d kv --bottommost force --threads 4,message: compact sucess!
2024-07-25 15:59:49,700 - root-main.py[line:941] - INFO - cmd:tiup ctl:v8.0.0 tikv --host 192.168.31.201:20160 compact -r 147 -c write -d kv --bottommost force --threads 4,message: compact sucess!
2024-07-25 15:59:49,700 - root-main.py[line:965] - INFO - start compact table:tpch.orders,total region count:3, current region num:2,region_id:157,peer_id:158
2024-07-25 15:59:49,745 - root-main.py[line:941] - INFO - cmd:tiup ctl:v8.0.0 tikv --host 192.168.31.201:20160 compact -r 157 -c default -d kv --bottommost force --threads 4,message: compact sucess!
2024-07-25 15:59:51,390 - root-main.py[line:941] - INFO - cmd:tiup ctl:v8.0.0 tikv --host 192.168.31.201:20160 compact -r 157 -c write -d kv --bottommost force --threads 4,message: compact sucess!
2024-07-25 15:59:51,391 - root-main.py[line:965] - INFO - start compact table:tpch.orders,total region count:3, current region num:3,region_id:129,peer_id:130
2024-07-25 15:59:51,433 - root-main.py[line:941] - INFO - cmd:tiup ctl:v8.0.0 tikv --host 192.168.31.201:20160 compact -r 129 -c default -d kv --bottommost force --threads 4,message: compact sucess!
2024-07-25 15:59:52,367 - root-main.py[line:941] - INFO - cmd:tiup ctl:v8.0.0 tikv --host 192.168.31.201:20160 compact -r 129 -c write -d kv --bottommost force --threads 4,message: compact sucess!
2024-07-25 15:59:52,368 - root-main.py[line:997] - INFO - tabname:tpch.customer, compact count:2, error ccompact count:0
2024-07-25 15:59:52,368 - root-main.py[line:997] - INFO - tabname:tpch.orders, compact count:6, error ccompact count:0
2024-07-25 15:59:52,368 - root-main.py[line:999] - INFO - Complete
```