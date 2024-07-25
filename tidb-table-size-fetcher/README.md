# 获取TiDB表实际存储空间大小信息

requirements:
- python3 (>=3.7) or python2 (>=2.7)
- sqlite3 (如果需要使用sqlite3存储数据)

## 使用方法
1. 查询一个集群中所有数据库的表大小信息
```text
(env) [tidb@host0 prometheus]$ python3 main.py -c tidb-test -d "*" -t "*"
DataBase  TabName     Partition  IndexCnt  DataSize   DataSizeF  Indexsize  IndexsizeF  Tablesize  TablesizeF  
tpcc      order_line  False      1         576462013  549.76MB   319017517  304.24MB    615827074  587.30MB    
tpcc      stock       False      1         506403010  482.94MB   284154647  270.99MB    533024838  508.33MB    
tpcc      customer    False      2         398387415  379.93MB   288555358  275.19MB    422317179  402.75MB    
tpcc      orders      False      2         0          0.00B      304281862  290.19MB    304281862  290.19MB    
tpcc      history     False      2         0          0.00B      296683864  282.94MB    296683864  282.94MB    
tpcc      item        False      0         292903779  279.33MB   0          0.00B       292903779  279.33MB    
tpcc      warehouse   False      0         287950156  274.61MB   0          0.00B       287950156  274.61MB    
tpcc      new_order   False      1         0          0.00B      274841712  262.11MB    274841712  262.11MB    
tpcc      district    False      1         0          0.00B      270041160  257.53MB    270041160  257.53MB    
tpch      lineitem    False      1         559014037  533.12MB   280706686  267.70MB    583657907  556.62MB    
tpch      orders      False      0         325636451  310.55MB   0          0.00B       325636451  310.55MB    
tpch      partsupp    False      1         272480704  259.86MB   281631213  268.58MB    298049101  284.24MB    
tpch      nation      False      0         277221576  264.38MB   0          0.00B       277221576  264.38MB    
tpch      region      False      0         277221576  264.38MB   0          0.00B       277221576  264.38MB    
tpch      part        False      0         277221576  264.38MB   0          0.00B       277221576  264.38MB    
tpch      supplier    False      0         277221576  264.38MB   0          0.00B       277221576  264.38MB    
tpch      customer    False      0         268366257  255.93MB   0          0.00B       268366257  255.93MB   
```
2. 查询一个库中所有表的大小信息
```text
(env) [tidb@host0 prometheus]$ python3 main.py -c tidb-test -d "tpch" -t "*"
DataBase  TabName   Partition  IndexCnt  DataSize   DataSizeF  Indexsize  IndexsizeF  Tablesize  TablesizeF  
tpch      lineitem  False      1         559014037  533.12MB   280706686  267.70MB    583657907  556.62MB    
tpch      orders    False      0         325636451  310.55MB   0          0.00B       325636451  310.55MB    
tpch      partsupp  False      1         272480704  259.86MB   281631213  268.58MB    298049101  284.24MB    
tpch      nation    False      0         277221576  264.38MB   0          0.00B       277221576  264.38MB    
tpch      region    False      0         277221576  264.38MB   0          0.00B       277221576  264.38MB    
tpch      part      False      0         277221576  264.38MB   0          0.00B       277221576  264.38MB    
tpch      supplier  False      0         277221576  264.38MB   0          0.00B       277221576  264.38MB    
tpch      customer  False      0         268366257  255.93MB   0          0.00B       268366257  255.93MB  
```
3. 查询一个表的大小信息
```text
(env) [tidb@host0 prometheus]$ python3 get_table_size.py -c tidb-test -d "tpch" -t "customer"
DataBase  TabName   Partition  IndexCnt  DataSize   DataSizeF  Indexsize  IndexsizeF  Tablesize  TablesizeF  
tpch      customer  False      0         268366257  255.93MB   0          0.00B       268366257  255.93MB 

# 忽略数据库名   
(env) [tidb@host0 prometheus]$ python3 get_table_size.py -c tidb-test -d "*" -t "customer"
DataBase  TabName   Partition  IndexCnt  DataSize   DataSizeF  Indexsize  IndexsizeF  Tablesize  TablesizeF  
tpcc      customer  False      2         398387415  379.93MB   288555358  275.19MB    422317179  402.75MB    
tpch      customer  False      0         268366257  255.93MB   0          0.00B       268366257  255.93MB   
```
