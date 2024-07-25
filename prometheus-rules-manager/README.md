# 修改prometheus的alert告警规则文件

在整体上支持两种修改方式：
- 修改单个配置文件的告警规则
- 在tiup中控机上批量修改所有集群（或指定集群）配置文件的告警规则

对于规则的修改支持三种方式：
- 新增一个告警规则
- 修改一个告警规则
- 删除一个告警规则

针对修改完成的告警规则，可以通过tiup cluster reload来重新加载配置文件，使得修改生效。

case1：修改单个配置文件的告警规则【删除一个告警规则】
```text
告警规则文件路径
filename=/home/tidb/software/tidb-community-toolkit-v7.4.0-linux-amd64/prometheus/tidb.rules.yml
cd /home/tidb/software/tidb-community-toolkit-v7.4.0-linux-amd64/prometheus/
# 查看告警规则个数
[tidb@host0 prometheus]$ grep -c " - alert" tidb.rules.yml 
13
[tidb@host0 prometheus]$ grep "TiDB_ddl_waiting_jobs" tidb.rules.yml 
  - alert: TiDB_ddl_waiting_jobs
  
删除其中的一个告警规则: TiDB_ddl_waiting_jobs
(env) [tidb@host0 tidb_prometheus_modify]$ python main.py -f /home/tidb/software/tidb-community-toolkit-v7.4.0-linux-amd64/prometheus/tidb.rules.yml delete --alert="TiDB_ddl_waiting_jobs"
2024-07-25 12:57:39,815 - INFO - main(261) - Using specified file
2024-07-25 12:57:39,836 - INFO - main(266) - Processing file /home/tidb/software/tidb-community-toolkit-v7.4.0-linux-amd64/prometheus/tidb.rules.yml
2024-07-25 12:57:39,846 - INFO - process_rule_file(178) - Successfully executed delete command on /home/tidb/software/tidb-community-toolkit-v7.4.0-linux-amd64/prometheus/tidb.rules.yml
2024-07-25 12:57:39,847 - INFO - main(272) - Backup files are stored in /tmp/prometheus_rules_backup/20240725_125739

再次检查告警规则个数
[tidb@host0 prometheus]$ grep -c " - alert" tidb.rules.yml 
12
[tidb@host0 prometheus]$ grep "TiDB_ddl_waiting_jobs" tidb.rules.yml 
[tidb@host0 prometheus]$ 

已经被成功删除
如果想恢复删除的告警规则，可以通过上述打印的日志中提供的恢复文件来做：
sh /tmp/prometheus_rules_backup/20240725_125739/rollback.sh
即可完成恢复
```
case2：批量修改tiup中控机上所有集群的告警规则【修改一个告警规则】
```text
将所有集群的tikv.rules.yml中的TiKV_server_report_failure_msg_total指标信息的告警级别从information修改为critical，并且将summary信息改为：测试
(env) [tidb@host0 tidb_prometheus_modify]$ python3 main.py --tiup modify --alert="TiKV_server_report_failure_msg_total" --set="labels.level=critical" --set="annotations.summary=\"测试\""
2024-07-25 13:07:39,004 - INFO - main(228) - Using tiup clusters
2024-07-25 13:07:39,040 - INFO - main(243) - Processing cluster tidb-test rule dir /home/tidb/software/tidb-community-toolkit-v7.4.0-linux-amd64/prometheus
2024-07-25 13:07:39,367 - INFO - process_rule_file(178) - Successfully executed modify command on /home/tidb/software/tidb-community-toolkit-v7.4.0-linux-amd64/prometheus/tikv.rules.yml
2024-07-25 13:07:39,367 - INFO - main(257) - 已经对集群[tidb-test]的规则文件进行了处理，请reload对应的监控组件使规则生效!
2024-07-25 13:07:39,367 - INFO - main(272) - Backup files are stored in /tmp/prometheus_rules_backup/20240725_130739
从上述日志中可以看到仅仅修改了tidb-test集群的tikv.rules.yml文件，其他集群的文件没有被修改，因为其它集群没有配置文件中没有告警规则

对于[tidb-test]集群，查看tikv.rules.yml中可以看到已经被修改：
[tidb@host0 prometheus]$ cat tikv.rules.yml |grep -A10 "TiKV_server_report_failure_msg_total"
  - alert: TiKV_server_report_failure_msg_total
    expr: sum(rate(tikv_server_report_failure_msg_total{type="unreachable"}[10m])) BY (store_id) > 10
    for: 1m
    labels:
      env: ENV_LABELS_ENV
      level: critical
      expr: sum(rate(tikv_server_report_failure_msg_total{type="unreachable"}[10m])) BY (store_id) > 10
    annotations:
      description: 'cluster: ENV_LABELS_ENV, instance: {{ $labels.instance }}, values:{{ $value }}'
      value: '{{ $value }}'
      summary: '"测试"'

对修改后的操作回滚：
sh /tmp/prometheus_rules_backup/20240725_130739/rollback.sh 

回滚后再次查看tikv.rules.yml中的告警规则，可以看到已经恢复：
[tidb@host0 prometheus]$ cat tikv.rules.yml |grep -A10 "TiKV_server_report_failure_msg_total"
  - alert: TiKV_server_report_failure_msg_total
    expr: sum(rate(tikv_server_report_failure_msg_total{type="unreachable"}[10m])) BY (store_id) > 10
    for: 1m
    labels:
      env: ENV_LABELS_ENV
      level: information
      expr: sum(rate(tikv_server_report_failure_msg_total{type="unreachable"}[10m])) BY (store_id) > 10
    annotations:
      description: 'cluster: ENV_LABELS_ENV, instance: {{ $labels.instance }}, values:{{ $value }}'
      value: '{{ $value }}'
      summary: TiKV server_report_failure_msg_total error
```
case3：批量修改指定集群的告警规则【新增2个告警规则】
```text
对tidb-test集群中新增两个告警规则，新增规则文件格式要和prometheus告警规则文件格式一致
这里我们对tidb.rules.yml中新增2个告警：
新增的告警文件为(一定要注意必须和原始文件层级关系一致，即包含groups,rules层级)：
[tidb@host0 tidb_prometheus_modify]$ cat tidb_rules_append.yml 
groups:
- name: alert.rules
  rules:  
  - alert: TiDB_ddl_waiting_jobs_append
    expr: sum(tidb_ddl_waiting_jobs) > 5
    for: 1m
    labels:
      env: ENV_LABELS_ENV
      level: warning
      expr: sum(tidb_ddl_waiting_jobs) > 5
    annotations:
      description: 'cluster: ENV_LABELS_ENV, instance: {{ $labels.instance }}, values:{{ $value }}'
      value: '{{ $value }}'
      summary: TiDB ddl waiting_jobs too much
  - alert: TiDB_node_restart_append
    expr: changes(process_start_time_seconds{job="tidb"}[5m]) > 0
    for: 1m
    labels:
      env: ENV_LABELS_ENV
      level: warning
      expr: changes(process_start_time_seconds{job="tidb"}[5m]) > 0
    annotations:
      description: 'cluster: ENV_LABELS_ENV, instance: {{ $labels.instance }}, values:{{ $value }}'
      value: '{{ $value }}'
      summary: TiDB server has been restarted
将其append到tidb.rules.yml文件中，在tidb_tikvclient_backoff_seconds_count指标后面
(env) [tidb@host0 tidb_prometheus_modify]$ python3 main.py --tiup append -f "tidb.rules.yml" -a "tidb_rules_append.yml" --after "tidb_tikvclient_backoff_seconds_count"
2024-07-25 14:09:44,822 - INFO - main(231) - Using tiup clusters
2024-07-25 14:09:44,867 - INFO - main(246) - Processing cluster tidb-test rule dir /home/tidb/software/tidb-community-toolkit-v7.4.0-linux-amd64/prometheus
2024-07-25 14:09:44,868 - INFO - main(254) - Backup file /home/tidb/software/tidb-community-toolkit-v7.4.0-linux-amd64/prometheus/tidb.rules.yml to /tmp/prometheus_rules_backup/20240725_140944
2024-07-25 14:09:44,898 - INFO - process_rule_file(179) - Successfully executed append command on /home/tidb/software/tidb-community-toolkit-v7.4.0-linux-amd64/prometheus/tidb.rules.yml
2024-07-25 14:09:44,899 - INFO - main(264) - 已经对集群[tidb-test]的规则文件进行了处理，请reload对应的监控组件使规则生效!
2024-07-25 14:09:44,899 - INFO - main(279) - Backup files are stored in /tmp/prometheus_rules_backup/20240725_140944


````
