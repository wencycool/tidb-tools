import pathlib
import yaml


def get_clusternames():
    """
    获取所有集群名称
    :rtype: list(str)
    """
    tiupdir = pathlib.Path("~/.tiup").expanduser()
    clusters = []
    clusters_dir = pathlib.Path(tiupdir).joinpath('storage/cluster/clusters')
    for cluster in clusters_dir.iterdir():
        if cluster.is_dir():
            clusters.append(cluster.name)
    return clusters


class ClusterInfo:
    def __init__(self, cname):
        """
        :param cname: 集群名称
        :type cname: str
        """
        self.cname = cname
        self.tiupdir = pathlib.Path("~/.tiup").expanduser()
        self.metadata = self.get_metadata()

    def get_metafile(self):
        """
        获取集群元数据文件路径
        :rtype: pathlib.Path
        """
        metafile = self.tiupdir.joinpath('storage/cluster/clusters').joinpath(self.cname).joinpath(
            'meta.yaml').absolute()
        return metafile

    def get_metadata(self):
        """
        获取集群元数据
        :rtype: dict
        """
        try:
            with open(self.get_metafile(), 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            raise Exception(f"Cluster {self.cname} not found")

    def get_alertmanager_url(self):
        """
        获取alertmanager的URL，一个集群只能有一个alertmanager
        :rtype: str
        """
        alertmanager_servers = self.metadata['topology']['alertmanager_servers']
        if len(alertmanager_servers) > 1:
            raise Exception("Only one alertmanager is supported")
        elif len(alertmanager_servers) == 0:
            raise Exception("No alertmanager found")
        return alertmanager_servers[0]['host'] + ":" + str(alertmanager_servers[0]['web_port'])
