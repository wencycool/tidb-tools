#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import pathlib
import shutil
import yaml
from datetime import datetime
from pkg.rules_manager import PrometheusRulesManager, AlertRuleNotFoundException, PrometheusRulesManagerException
from pkg.logger import Logger

logger = Logger(__name__)


# 在对文件修改前需要将文件备份到指定的路径下，备份文件中存放每次修改的前一个版本信息和回退方法
# 生成rollback.sh文件，用于回退到上一个版本，里面存放反向cp操作
def backup_file(abs_file, backup_dir, rollback_file='rollback.sh', comment=''):
    """
    将abs_file备份到backup_dir中，并在rollback_file中写入回退操作
    :param abs_file: 要备份的文件的绝对路径
    :type abs_file: str
    :param backup_dir: 备份文件的目录
    :type backup_dir: str
    :param rollback_file: 回退脚本文件
    :type rollback_file: str
    :return: 是否备份成功
    :rtype: bool
    """
    # 将路径转化成___生成新的文件名（包含转化后的路径信息），如此一来可以扁平的将文件放到一个文件夹中，同时在rollback_file中写入回退操作
    try:
        rollback_file = pathlib.Path(backup_dir).joinpath(rollback_file)
        new_file = pathlib.Path(backup_dir).joinpath(abs_file.replace('/', '___'))
        new_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(abs_file, new_file)
        # 保持权限相同
        source_permissions = pathlib.Path(abs_file).stat().st_mode
        new_file.chmod(source_permissions)
        with open(rollback_file, 'a') as f:
            if comment:
                f.write(f'# {comment}\n')
            f.write(f'cp {new_file} {abs_file}\n')
    except Exception as e:
        logger.error(f'Error: {e}')
        return False
    return True


def get_tiup_install_dir():
    """
    获取tiup的安装目录
    :return: tiup的安装目录
    :rtype: pathlib.Path
    """
    try:
        command_file = shutil.which("tiup")
        if not command_file:
            raise Exception("tiup not found")
        install_dir = pathlib.Path(command_file).parent.parent.resolve()
    except Exception:
        raise Exception("tiup not found")
    return install_dir


def get_tiup_clusters_rule_dir(cluster_names=None):
    """
    获取tiup集群的规则文件目录，如果指定了cluster_names，则只返回指定集群的规则文件目录，
    一个集群有可能配置了多个监控节点，因此一个集群可能返回多个规则目录
    :param cluster_names: 集群名称列表
    :type cluster_names: list
    :return: {cluster_name: [rule_dir1, rule_dir2, ...]}
    :rtype: dict
    """
    search_paths = get_tiup_install_dir().joinpath("storage/cluster/clusters")
    logger.debug(f"tiup配置文件查找集群路径: {search_paths}")
    rule_dir_map = {}
    local_clusters = [cluster.name for cluster in search_paths.iterdir() if cluster.is_dir()]
    logger.debug(f"tiup配置文件中找到的集群：{','.join(local_clusters)}")
    if cluster_names:
        for cluster_name in cluster_names:
            if cluster_name not in local_clusters:
                raise Exception(f"Cluster {cluster_name} not found")
            meta_file = search_paths.joinpath(cluster_name, "meta.yaml")
            logger.debug(f"Cluster {cluster_name} meta file: {meta_file}")
            if meta_file.exists():
                rule_dir_map[cluster_name] = get_rule_dirs(str(meta_file.resolve()))
            else:
                logger.warning(f"Meta file not found for cluster {cluster_name}")
    else:
        for cluster_name in local_clusters:
            meta_file = search_paths.joinpath(cluster_name, "meta.yaml")
            logger.debug(f"Cluster {cluster_name} meta file: {meta_file}")
            if meta_file.exists():
                rule_dir_map[cluster_name] = get_rule_dirs(str(meta_file.resolve()))
            else:
                logger.warning(f"Meta file not found for cluster {cluster_name}")
    return rule_dir_map


def get_rule_dirs(meta_file):
    """
    从meta.yaml文件中找到rules_dir
    :param meta_file: meta.yaml文件的路径
    :type meta_file: str
    :return: rules_dir列表
    :rtype: list
    """
    rule_dirs = []
    with open(meta_file, "r", encoding="utf-8") as file:
        meta = yaml.safe_load(file)
    try:
        for server in meta["topology"]["monitoring_servers"]:
            if server.get("rule_dir"):
                rule_dirs.append(server["rule_dir"])
    except KeyError:
        logger.warning(f"rule_dir not found in {meta_file}")
    return rule_dirs


def check_alert_name_in_yaml_file(alert_file, alert_name):
    """
    检查一个alert_name是否在规则文件中存在
    :param alert_file: 规则文件的路径
    :type alert_file: str
    :param alert_name: alert的名称
    :type alert_name: str
    :return: 是否存在
    :rtype: bool
    """
    try:
        logger.debug(f"Checking alert {alert_name} in {alert_file}")
        prometheus_rules_manager = PrometheusRulesManager(alert_file)
        # 判断文件名是否在规则文件中，避免一些非规则文件但是符合解析的文件被误判
        if not prometheus_rules_manager.is_rulefile():
            return False
        PrometheusRulesManager(alert_file).find_alert_index(alert_name)
        return True
    except AlertRuleNotFoundException:
        logger.debug(f"Alert {alert_name} not found in {alert_file}")
        return False
    except PrometheusRulesManagerException as e:
        logger.debug(f"{e}")
        return False


def check_alert_name_in_rule_dir(rule_dir, alert_name):
    """
    检查一个alert_name是否在规则文件中存在
    :param rule_dir: 规则文件目录
    :type rule_dir: str
    :param alert_name: alert的名称
    :type alert_name: str
    :return: 规则文件的路径
    :rtype: str
    """
    logger.debug(f"Checking alert {alert_name} in {rule_dir}")
    for rule_file in pathlib.Path(rule_dir).rglob("*.yml"):
        logger.debug(f"Checking {rule_file}")
        if check_alert_name_in_yaml_file(str(rule_file), alert_name):
            return str(rule_file)
    raise AlertRuleNotFoundException(alert_name)


def process_rule_file(manager, command, args):
    """
    处理 Prometheus 规则文件
    :param manager: PrometheusRulesManager 实例
    :param command: 要执行的命令
    :param args: 参数
    """
    try:
        if command == 'delete':
            manager.delete_alert_rule(args.alert)
        elif command == 'modify':
            modifications = {item.split('=', 1)[0].strip(): item.split('=', 1)[1].strip() for item in args.set}
            manager.modify_alert_rule(args.alert, modifications)
        elif command == 'append':
            manager.append_alert_rules(args.append_file, after=args.after)
        manager.save_rules()
        logger.info(f'Successfully executed {command} command on {manager.file_path}')
    except Exception as e:
        logger.error(f'Error: {e}')
        # logger.error(traceback.format_exc())
        return False
    return True


def main():
    parser = argparse.ArgumentParser(description='修改prometheus的规则文件')
    parser.add_argument('--debug', action='store_true', help='开启debug模式')
    parser.add_argument('-v', '--version', action='version', version='%(prog)s 1.0')
    parser.add_argument('--backup_dir', type=str, default='/tmp/prometheus_rules_backup',
                        help='备份文件的路径，如果指定该路径，那么该路径下会存放每次修改的前一个版本信息和回退方法')
    parser.add_argument('-f', '--file', help='prometheus规则文件的路径')
    parser.add_argument('-t', '--tiup', action='store_true', help='使用tiup集群')
    parser.add_argument('-c', '--clusters', help='指定的集群名称，多个集群名称用逗号分隔')

    subparsers = parser.add_subparsers(dest='command', required=True)
    append_parser = subparsers.add_parser('append', help='添加一个或多个alert规则')
    append_parser.add_argument('-f', '--file', required=False,
                               help='prometheus规则文件名，只需要填写文件名，不需要路径，append_file会被添加到该文件')
    append_parser.add_argument('-a', '--append_file', required=True,
                               help='被添加的规则文件路径，与被追加的规则文件格式一致')
    append_parser.add_argument('--after', required=False, help='追加在哪个alert规则之后，如果不指定则追加在最后')

    delete_parser = subparsers.add_parser('delete', help='删除一个alert规则')
    delete_parser.add_argument('--alert', required=True, help='要删除的alert规则的名称')

    modify_parser = subparsers.add_parser('modify', help='修改一个alert规则')
    modify_parser.add_argument('--alert', required=True, help='要修改的alert规则的名称')
    modify_parser.add_argument('--set', action='append', required=True,
                               help='要修改的属性值，格式为key=value，可以多次指定，举例：--set for=10m --set '
                                    'annotations.summary=xxx，注意key不用双引号，要整体用引号包裹，如--set "annotations.summary=xxx"')

    args = parser.parse_args()

    if args.debug:
        logger.setDebugLevel()

    global backup_dir
    # 检查参数互斥关系
    if args.command != "append" and args.file and (args.tiup or args.clusters):
        parser.error("The argument -f cannot be used with -t or -c.")
    if args.clusters and not args.tiup:
        parser.error("The argument -c can only be used with -t.")
    if args.backup_dir:
        base_backup_dir = pathlib.Path(args.backup_dir)
        base_backup_dir.mkdir(parents=True, exist_ok=True)
        backup_dir = base_backup_dir.joinpath(datetime.now().strftime('%Y%m%d_%H%M%S'))
        backup_dir.mkdir(parents=True, exist_ok=True)
    if args.tiup:
        logger.info("Using tiup clusters")
        clusters = [x.strip() for x in args.clusters.split(',')] if args.clusters else None
        rule_dir_map = get_tiup_clusters_rule_dir(clusters)
        if clusters:
            logger.debug(f"Clusters: {','.join(clusters)}")
        else:
            logger.debug("All clusters")
        # 真正已经做规则处理的集群
        clusters_with_rules = []
        if not rule_dir_map:
            raise Exception("No rule files found")
        for cluster, rule_dirs in rule_dir_map.items():
            logger.debug(f"Cluster [{cluster}] rule dirs: {','.join(rule_dirs)}")
            for rule_dir in rule_dirs:
                try:
                    logger.info(f"Processing cluster {cluster} rule dir {rule_dir}")
                    if args.file:
                        possible_yaml_file = str(pathlib.Path(rule_dir).joinpath(pathlib.Path(args.file).name))
                    else:
                        possible_yaml_file = check_alert_name_in_rule_dir(rule_dir, args.alert)
                    if not pathlib.Path(possible_yaml_file).is_file():
                        logger.warning(f"File {possible_yaml_file} not found in cluster {cluster}")
                        continue
                    logger.info(f"Backup file {possible_yaml_file} to {backup_dir}")
                    if not backup_file(possible_yaml_file, str(backup_dir), comment=f"Cluster: {cluster}"):
                        logger.error(f"Backup failed for {possible_yaml_file}")
                    else:
                        manager = PrometheusRulesManager(possible_yaml_file)
                        if process_rule_file(manager, args.command, args):
                            clusters_with_rules.append(cluster)
                except AlertRuleNotFoundException:
                    logger.warning(f"Alert {args.alert} not found in cluster {cluster}")
        if clusters_with_rules:
            logger.info(
                f'已经对集群[{",".join(clusters_with_rules)}]的规则文件进行了处理，请reload对应的监控组件使规则生效!')
    else:
        try:
            logger.info("Using specified file")
            if not backup_file(args.file, str(backup_dir)):
                logger.error(f"Backup failed for {args.file}")
            else:
                manager = PrometheusRulesManager(args.file)
                logger.info(f"Processing file {args.file}")
                process_rule_file(manager, args.command, args)
        except Exception as e:
            logger.error(f'Error: {e}')
            exit(1)
    if backup_dir.is_dir() and list(backup_dir.iterdir()):
        logger.info(f'Backup files are stored in {backup_dir}')


if __name__ == '__main__':
    main()
