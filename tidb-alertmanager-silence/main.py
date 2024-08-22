from pkg.silence import SilenceManager
from pkg.utils import ClusterInfo, get_clusternames
from datetime import datetime
import argparse
import logging as log


def main():
    parser = argparse.ArgumentParser(description="Alertmanager Silence")
    parser.add_argument("-cs", "--clusters", help="集群名称列表，用逗号分隔，不填写代表当前tiup上所有集群", required=False)
    parser.add_argument("--roles", help="tidb节点类型名称列表，以逗号分隔（如：\"tidb,pd,tikv\"），如果不设置代表屏蔽当前集群所有告警", required=False)
    sub_parser = parser.add_subparsers(dest="action", help="操作类型")
    create_parser = sub_parser.add_parser("create", help="创建silence")
    create_parser.add_argument("--startsAt", help="开始时间，时间格式为:2024-07-20-18:00:00", required=True)
    create_parser.add_argument("--endsAt", help="结束时间，时间格式为:2024-07-20-18:30:00", required=True)

    delete_parser = sub_parser.add_parser("delete", help="删除silence")
    delete_parser.add_argument("--silenceid", help="silence id，如果不指定则默认删除当前集群下所有的屏蔽告警",
                               required=False)
    list_parser = sub_parser.add_parser("list", help="列出当前活动状态的silence")

    args = parser.parse_args()
    log.basicConfig(filename=None, level=log.INFO,
                    format='%(asctime)s - %(name)s-%(filename)s[line:%(lineno)d] - %(levelname)s - %(message)s')

    if not args.roles:
        roles = []
    else:
        roles = [role.strip() for role in args.roles.split(",")]
    if not args.clusters:
        log.info("将操作所有集群")
        clusters = get_clusternames()
    else:
        clusters = args.clusters.split(",")
        log.info(f"集群名称: {clusters}")
        # 校验集群名称是否存在
        if not set(clusters).issubset(set(get_clusternames())):
            log.error(f"集群名称存在错误，请检查集群名称是否正确")
    for clustername in clusters:
        try:
            cluster = ClusterInfo(clustername)
            alertmanager_url = cluster.get_alertmanager_url()
            log.info(f"Cluster:[{clustername}],alertmanager url: [{alertmanager_url}]")
            sm = SilenceManager(alertmanager_url)
            if args.action == "create":
                startsAt = datetime.strptime(args.startsAt, "%Y-%m-%d-%H:%M:%S")
                endsAt = datetime.strptime(args.endsAt, "%Y-%m-%d-%H:%M:%S")
                silenceids = sm.create_silence(roles, startsAt, endsAt)
                log.info(f"Cluster:[{clustername}],silence ids: [{','.join(silenceids)}],silence created success!")
            elif args.action == "delete":
                if args.silenceid:
                    sm.delete_silence(args.silenceid)
                    log.info(f"Cluster:[{clustername}],silence id: [{args.silenceid}],silence deleted success!")
                else:
                    log.info(f"Cluster:[{clustername}],delete all silence")
                    silences = sm.list_silences()
                    if len(silences) != 0:
                        log.info(f"Delete silence loop...")
                    for silence in silences:
                        silence_id = silence.get("id")
                        sm.delete_silence(silence_id)
                        log.info(f"    Silence deleted,silence id: {silence_id}")
                    log.info(f"Cluster:[{clustername}],all silence deleted success!")
            elif args.action == "list":
                silences = sm.list_silences()
                for silence in silences:
                    log.info(f"Cluster:[{clustername}],silences_format: {sm.silence_format(silence)}")
        except Exception as e:
            log.error(f"Cluster {clustername} error: {str(e)}")


if __name__ == "__main__":
    main()
