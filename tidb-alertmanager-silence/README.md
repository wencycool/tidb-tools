# TiDB的alertmanager告警静默
requirements:
- python3 (>=3.7)

## 1. 背景
TiDB集群中的alertmanager会定时检查集群中的各种告警，如果有告警则会发送通知给相关人员。但是有时候我们并不希望所有的告警都发送通知，比如例行做变更维护时做集群或者节点重启，为避免告警的干扰。这时候我们可以通过alertmanager的silence功能来实现告警的静默。

## 2. 静默告警的方式
- 通过alertmanager的api方式来静默告警（无权限认证）
- 通过在整个集群级别过滤alertname来屏蔽告警，如果不填写任何过滤条件，则表示屏蔽整个集群的告警
注意：目前只支持屏蔽整个集群的某个告警，不支持屏蔽某个节点或者某个实例的告警
## 3. 静默告警的时间
- 支持静默告警的时间段，如从某个时间点开始静默告警，到某个时间点结束静默告警或从某个时间开始持续静默告警一段时间
- 按照本地时区设置即可，会自动转换为UTC时间

使用方法：
case1：静默所有集群的所有告警
静默本地时间2024-07-28 20:00:00到2024-07-28 23:00:00的所有告警
```text
(venv) [tidb@host0 tidb_silence]$ python main.py create --startsAt "2024-07-28-20:00:00" --endsAt "2024-07-28-23:00:00" 
2024-07-28 20:21:14,515 - root-main.py[line:27] - INFO - 将操作所有集群
2024-07-28 20:21:14,534 - root-main.py[line:39] - INFO - Cluster:[tidb-test2],alertmanager url: [192.168.31.100:9093]
2024-07-28 20:21:17,648 - root-main.py[line:65] - ERROR - Cluster tidb-test2 error: HTTPConnectionPool(host='192.168.31.100', port=9093): Max retries exceeded with url: /api/v2/silences (Caused by NewConnectionError('<urllib3.connection.HTTPConnection object at 0x7f93edc4e070>: Failed to establish a new connection: [Errno 113] No route to host'))
2024-07-28 20:21:17,659 - root-main.py[line:39] - INFO - Cluster:[tidb-test1],alertmanager url: [192.168.31.201:9193]
2024-07-28 20:21:17,661 - root-main.py[line:45] - INFO - Cluster:[tidb-test1],silence id: [adc93904-1777-43fa-8f3b-4afa7ed59068],silence created success!
2024-07-28 20:21:17,667 - root-main.py[line:39] - INFO - Cluster:[tidb-test],alertmanager url: [192.168.31.201:9093]
2024-07-28 20:21:17,669 - root-main.py[line:45] - INFO - Cluster:[tidb-test],silence id: [8bd322a9-e045-4afa-a870-38bdb84a3991],silence created success!
```
case2：静默tiup上所有集群的某个告警
```text
(venv) [tidb@host0 tidb_silence]$ python main.py create --alertname="TiKV_server_is_down" --startsAt "2024-07-28-20:00:00" --endsAt "2024-07-28-23:00:00" 
2024-07-28 20:11:33,066 - root-main.py[line:27] - INFO - 将操作所有集群
2024-07-28 20:11:33,085 - root-main.py[line:39] - INFO - Cluster:[tidb-test2],alertmanager url: [192.168.31.100:9093]
2024-07-28 20:11:36,208 - root-main.py[line:65] - ERROR - Cluster tidb-test2 error: HTTPConnectionPool(host='192.168.31.100', port=9093): Max retries exceeded with url: /api/v2/silences (Caused by NewConnectionError('<urllib3.connection.HTTPConnection object at 0x7f4fbe9ee040>: Failed to establish a new connection: [Errno 113] No route to host'))
2024-07-28 20:11:36,219 - root-main.py[line:39] - INFO - Cluster:[tidb-test1],alertmanager url: [192.168.31.201:9193]
2024-07-28 20:11:36,221 - root-main.py[line:45] - INFO - Cluster:[tidb-test1],silence id: [e379cb3c-10d7-43c2-a647-802b0de28188],silence created success!
2024-07-28 20:11:36,228 - root-main.py[line:39] - INFO - Cluster:[tidb-test],alertmanager url: [192.168.31.201:9093]
2024-07-28 20:11:36,230 - root-main.py[line:45] - INFO - Cluster:[tidb-test],silence id: [8a41524e-f855-4d45-9ae9-1a112d96db28],silence created success!
```
其中error部分是因为alertmanager的api地址不通导致的，可以忽略

case3：静默某个集群的某个告警的正则匹配
```text
(venv) [tidb@host0 tidb_silence]$ python main.py create --alertname="TiDB.*" --startsAt "2024-07-28-20:00:00" --endsAt "2024-07-28-23:00:00" 
2024-07-28 20:14:17,258 - root-main.py[line:27] - INFO - 将操作所有集群
2024-07-28 20:14:17,277 - root-main.py[line:39] - INFO - Cluster:[tidb-test2],alertmanager url: [192.168.31.100:9093]
2024-07-28 20:14:20,368 - root-main.py[line:65] - ERROR - Cluster tidb-test2 error: HTTPConnectionPool(host='192.168.31.100', port=9093): Max retries exceeded with url: /api/v2/silences (Caused by NewConnectionError('<urllib3.connection.HTTPConnection object at 0x7f8495fb0040>: Failed to establish a new connection: [Errno 113] No route to host'))
2024-07-28 20:14:20,380 - root-main.py[line:39] - INFO - Cluster:[tidb-test1],alertmanager url: [192.168.31.201:9193]
2024-07-28 20:14:20,383 - root-main.py[line:45] - INFO - Cluster:[tidb-test1],silence id: [c203234f-d58c-4128-b2f7-1d789db26efb],silence created success!
2024-07-28 20:14:20,394 - root-main.py[line:39] - INFO - Cluster:[tidb-test],alertmanager url: [192.168.31.201:9093]
2024-07-28 20:14:20,397 - root-main.py[line:45] - INFO - Cluster:[tidb-test],silence id: [4d6f2f97-e550-4f5e-b26b-07487ac7e711],silence created success!
```
case4：查看所有集群的所有告警静默
```text
(venv) [tidb@host0 tidb_silence]$ python main.py list 
2024-07-28 20:15:16,516 - root-main.py[line:27] - INFO - 将操作所有集群
2024-07-28 20:15:16,535 - root-main.py[line:39] - INFO - Cluster:[tidb-test2],alertmanager url: [192.168.31.100:9093]
2024-07-28 20:15:19,632 - root-main.py[line:65] - ERROR - Cluster tidb-test2 error: HTTPConnectionPool(host='192.168.31.100', port=9093): Max retries exceeded with url: /api/v2/silences (Caused by NewConnectionError('<urllib3.connection.HTTPConnection object at 0x7fad1849f280>: Failed to establish a new connection: [Errno 113] No route to host'))
2024-07-28 20:15:19,643 - root-main.py[line:39] - INFO - Cluster:[tidb-test1],alertmanager url: [192.168.31.201:9193]
2024-07-28 20:15:19,646 - root-main.py[line:63] - INFO - Cluster:[tidb-test1],silences_format: SilenceID: e379cb3c-10d7-43c2-a647-802b0de28188, CreatedBy: system, StartsAt: 2024-07-28T12:11:36.216Z, EndsAt: 2024-07-28T15:00:00.000Z, Comment: auto silence
2024-07-28 20:15:19,646 - root-main.py[line:63] - INFO - Cluster:[tidb-test1],silences_format: SilenceID: c203234f-d58c-4128-b2f7-1d789db26efb, CreatedBy: system, StartsAt: 2024-07-28T12:14:20.377Z, EndsAt: 2024-07-28T15:00:00.000Z, Comment: auto silence
2024-07-28 20:15:19,654 - root-main.py[line:39] - INFO - Cluster:[tidb-test],alertmanager url: [192.168.31.201:9093]
2024-07-28 20:15:19,656 - root-main.py[line:63] - INFO - Cluster:[tidb-test],silences_format: SilenceID: 365485eb-1d8d-453f-af96-e75fbaf1bbe3, CreatedBy: system, StartsAt: 2024-07-28T12:11:15.136Z, EndsAt: 2024-07-28T15:00:00.000Z, Comment: auto silence
2024-07-28 20:15:19,657 - root-main.py[line:63] - INFO - Cluster:[tidb-test],silences_format: SilenceID: 1edb0643-ca86-4b6a-b040-f79b4993ed7a, CreatedBy: system, StartsAt: 2024-07-28T12:09:46.546Z, EndsAt: 2024-07-28T15:00:00.000Z, Comment: auto silence
2024-07-28 20:15:19,657 - root-main.py[line:63] - INFO - Cluster:[tidb-test],silences_format: SilenceID: 4d6f2f97-e550-4f5e-b26b-07487ac7e711, CreatedBy: system, StartsAt: 2024-07-28T12:14:20.391Z, EndsAt: 2024-07-28T15:00:00.000Z, Comment: auto silence
2024-07-28 20:15:19,657 - root-main.py[line:63] - INFO - Cluster:[tidb-test],silences_format: SilenceID: 8a41524e-f855-4d45-9ae9-1a112d96db28, CreatedBy: system, StartsAt: 2024-07-28T12:11:36.224Z, EndsAt: 2024-07-28T15:00:00.000Z, Comment: auto silence
(venv) [tidb@host0 tidb_silence]$ 
```
case5：删除某个集群的所有告警静默
```text
(venv) [tidb@host0 tidb_silence]$ python main.py delete
2024-07-28 20:15:56,152 - root-main.py[line:27] - INFO - 将操作所有集群
2024-07-28 20:15:56,171 - root-main.py[line:39] - INFO - Cluster:[tidb-test2],alertmanager url: [192.168.31.100:9093]
2024-07-28 20:15:56,171 - root-main.py[line:51] - INFO - Cluster:[tidb-test2],delete all silence
2024-07-28 20:15:59,248 - root-main.py[line:65] - ERROR - Cluster tidb-test2 error: HTTPConnectionPool(host='192.168.31.100', port=9093): Max retries exceeded with url: /api/v2/silences (Caused by NewConnectionError('<urllib3.connection.HTTPConnection object at 0x7f166a249280>: Failed to establish a new connection: [Errno 113] No route to host'))
2024-07-28 20:15:59,259 - root-main.py[line:39] - INFO - Cluster:[tidb-test1],alertmanager url: [192.168.31.201:9193]
2024-07-28 20:15:59,259 - root-main.py[line:51] - INFO - Cluster:[tidb-test1],delete all silence
2024-07-28 20:15:59,261 - root-main.py[line:54] - INFO - Delete since loop...
2024-07-28 20:15:59,263 - root-main.py[line:58] - INFO -     Silence deleted,silence id: e379cb3c-10d7-43c2-a647-802b0de28188
2024-07-28 20:15:59,265 - root-main.py[line:58] - INFO -     Silence deleted,silence id: c203234f-d58c-4128-b2f7-1d789db26efb
2024-07-28 20:15:59,266 - root-main.py[line:59] - INFO - Cluster:[tidb-test1],all silence deleted success!
2024-07-28 20:15:59,276 - root-main.py[line:39] - INFO - Cluster:[tidb-test],alertmanager url: [192.168.31.201:9093]
2024-07-28 20:15:59,277 - root-main.py[line:51] - INFO - Cluster:[tidb-test],delete all silence
2024-07-28 20:15:59,279 - root-main.py[line:54] - INFO - Delete since loop...
2024-07-28 20:15:59,281 - root-main.py[line:58] - INFO -     Silence deleted,silence id: 365485eb-1d8d-453f-af96-e75fbaf1bbe3
2024-07-28 20:15:59,282 - root-main.py[line:58] - INFO -     Silence deleted,silence id: 4d6f2f97-e550-4f5e-b26b-07487ac7e711
2024-07-28 20:15:59,284 - root-main.py[line:58] - INFO -     Silence deleted,silence id: 8a41524e-f855-4d45-9ae9-1a112d96db28
2024-07-28 20:15:59,286 - root-main.py[line:58] - INFO -     Silence deleted,silence id: 1edb0643-ca86-4b6a-b040-f79b4993ed7a
2024-07-28 20:15:59,286 - root-main.py[line:59] - INFO - Cluster:[tidb-test],all silence deleted success!
```








