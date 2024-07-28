# TiDB工具集
每一个目录一个工具，相互之间不依赖
每个工具入口为main文件（shell则为run.sh）

工具列表；
- [prometheus-rules-manager](./prometheus-rules-manager)：Prometheus告警规则管理工具，支持批量增删改告警规则
- [tidb-alertmanager-silence](./tidb-alertmanager-silence)：TiDB告警静默工具，支持批量增删查改告警静默
- [tidb-compact-table](./tidb-compact-table)：针对表做Compaction操作，支持对指定多张表做Compaction操作
- [tidb-config-diff-checker](./tidb-config-diff-checker)：TiDB集群配置差异检查工具，支持检查多个TiDB集群配置差异
- [tidb-runstats-analyze](./tidb-runstats-analyze)：TiDB集群RunStats分析工具，支持分析TiDB集群RunStats信息，作为系统自动统计信息搜集的补充策略
- [tidb-table-size-fetcher](./tidb-table-size-fetcher)：TiDB集群表大小获取工具，支持获取TiDB集群表大小信息，该表大小接近实际存储大小（压缩后）

 ## 使用方法
```shell
cd <工具目录>
python3 -v venv venv
source venv/bin/activate
pip install -r requirements.txt
python main.py
```
具体使用方法见各工具目录下的`README.md`说明
