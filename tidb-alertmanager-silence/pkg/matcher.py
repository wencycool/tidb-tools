from enum import Enum


class SilenceType(Enum):
    """
    定义支持的silence类型，每种类型对应一系列告警名称（抑制这些告警后即可避免该类型节点的告警），用于生成silence的matchers对象
    """
    cluster = [".+"]  # 利用正则表达式匹配所有告警，表示整个集群都不进行告警
    pump = ["Pump_server_is_down", "binlog_drainer_checkpoint_tso_no_change_for.+"]
    drainer = ["Drainer_server_is_down"]
    tidb = ["TiDB_server_is_down", "TiDB_monitor_keep_alive", "TiDB_node_restart"]
    tikv = ["TiKV_server_report_failure_msg_total", "PD_cluster_lost_connect_tikv_nums",
            "PD_pending_peer_region_count", "TiKV_server_is_down", "TiDB_monitor_keep_alive",
            "tidb_tikvclient_backoff_seconds_count", "TiKV_node_restart"]
    tiflash = ["TiFlash_server_is_down", "PD_cluster_lost_connect_tikv_nums"]  # tiflash没有 "TiFlash_node_restart" 告警
    pd = ["PD_server_is_down", "PD_leader_change"]


class Matcher:
    """
    根据支持的silence类型，生成silence的matchers对象，支持链式调用来新增更多的SilenceType，例如:
    matchers = Matcher().add(SilenceType.cluster).add(SilenceType.pump).add(SilenceType.drainer)
    matchers.to_json() # 返回json格式的matchers对象
    """

    def __init__(self):
        self.matchers = []
        self.alertnames = []  # 用于存储已经添加的alertname，避免重复添加

    def add(self, silence_type):
        """
        添加silence类型，在现有的matchers中查找是否已经存在该类型，如果不存在则添加
        :type silence_type: SilenceType
        """
        for altername in silence_type.value:
            if altername not in self.alertnames:
                self.matchers.append({
                    "name": "alertname",
                    "value": altername,
                    "isRegex": True,
                })
                self.alertnames.append(altername)
        return self

    def add_alertname(self, alertname):
        """
        添加alertname，如果alertname已经存在则不添加
        :type alertname: str
        """
        if alertname not in self.alertnames:
            self.matchers.append({
                "name": "alertname",
                "value": alertname,
                "isRegex": True,
            })
            self.alertnames.append(alertname)
        return self

    def to_json(self):
        return {
            "matchers": self.matchers
        }

    def __str__(self):
        return str(self.to_json())

def split_matchers(matchers):
    """
    将matchers对象拆分为多个matcher类型
    :type matchers: Matcher
    :rtype: list
    """
    result_matchers = []
    for each_altername in matchers.alertnames:
       result_matchers.append(Matcher().add_alertname(each_altername))
    return result_matchers