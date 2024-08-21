# coding=utf8
import requests
from datetime import datetime, timedelta
import sys
import json
from urllib.parse import urljoin

from .matcher import Matcher, SilenceType

# coding=utf8
import requests
from datetime import datetime, timedelta
import sys
import json
from urllib.parse import urljoin

from .matcher import Matcher, SilenceType

# 创建silence类型，目前支持整个集群的silence或者某一个组件的silence，即某一个组件如果宕机后不再发送告警


# 创建silence Exception
class SilenceError(Exception):
    def __init__(self, message):
        self.message = message


def local2utc(local_st):
    """
    将本地时间转换为UTC时间
    Args:
        local_st (datetime): 本地时间
    Returns:
        datetime: 转换后的UTC时间
    """
    # 如果当前版本是3.9以及以上则直接用自带的zoneinfo包，否则使用backports.zoneinfo
    if sys.version_info < (3, 9):
        import pytz
        local_timezone = pytz.timezone("Asia/Shanghai")
        local_dt = local_timezone.localize(local_st)
        utc_dt = local_dt.astimezone(pytz.utc)
        return utc_dt
    else:
        try:
            from zoneinfo import ZoneInfo
            local_timezone = ZoneInfo("Asia/Shanghai")
            local_dt = local_st.replace(tzinfo=local_timezone)
            utc_dt = local_dt.astimezone(ZoneInfo("UTC"))
            return utc_dt
        except ImportError:
            raise ImportError("Please install backports.zoneinfo")


class SilenceManager:
    """
    Silence类，仅仅支持整个集群的silence或者某一个alertname的silence，alertname支持正则表达式形式（golang的正则形式）
    """
    def __init__(self, url):
        self.url = url if url.startswith("http") else "http://" + url
        self.timeout = 5

    def __headers(self):
        return {
            "Content-Type": "application/json",
        }
    def __generate_data(self, matcher, startsAt=datetime.now(),
                        endsAt=datetime.now() + timedelta(minutes=30), createdBy='system',
                        comment='auto silence'):
        """
        生成data对象
        :param matcher: 匹配规则
        :type matcher: Matcher
        :param startsAt:
        :type startsAt: datetime
        :param endsAt:
        :type endsAt: datetime
        :param createdBy:
        :type createdBy: str
        :param comment:
        :type comment: str
        :rtype: str
        """
        # 将当前时间转为UTC时间
        startsAt = local2utc(startsAt)
        endsAt = local2utc(endsAt)

        data = {
            "matchers": matcher.matchers,
            "startsAt": startsAt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "endsAt": endsAt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "createdBy": createdBy,
            "comment": comment,
        }
        return json.dumps(data)

    def create_silence(self, tidb_roles, startsAt, endsAt):
        """
        创建silence
        :param tidb_roles: TiDB集群中的组件名称，如：["tidb", "pd", "tikv","pump","drainer"]，目前值支持整个集群的silence
        :type tidb_roles: list
        :param startsAt:
        :type startsAt: datetime
        :param endsAt:
        :type endsAt: datetime
        :rtype: dict
        """
        matcher = Matcher()
        if not tidb_roles:
            matcher.add(SilenceType.cluster)
        else:
            for role in tidb_roles:
                role = role.lower()
                if role == "cluster":
                    matcher.add(SilenceType.cluster)
                elif role == "tidb":
                    matcher.add(SilenceType.tidb)
                elif role == "tikv":
                    matcher.add(SilenceType.tikv)
                elif role == "pd":
                    matcher.add(SilenceType.pd)
                elif role == "tiflash":
                    matcher.add(SilenceType.tiflash)
                elif role == "pump":
                    matcher.add(SilenceType.pump)
                elif role == "drainer":
                    matcher.add(SilenceType.drainer)
                else:
                    raise ValueError("Invalid role type")
        # fixme 这里的matchers是多个指标并且的意思，需要修改为多个告警抑制
        data = self.__generate_data(matcher, startsAt, endsAt)
        url = urljoin(self.url, "api/v2/silences")
        response = requests.post(url, headers=self.__headers(), data=data,
                                 timeout=self.timeout)
        if response.status_code == 200:
            return dict(json.loads(response.text)).get("silenceID")
        else:
            raise SilenceError(response.text)

    def list_silences(self):
        """
        列出silence
        :rtype: dict
        """
        response = requests.get(urljoin(self.url, "api/v2/silences"), headers=self.__headers(), timeout=self.timeout)
        if response.status_code == 200:
            silences = json.loads(response.text)
            active_silences = [silence for silence in silences if silence.get("status", {}).get("state") != "expired"]
            return active_silences
        else:
            raise SilenceError(response.text)

    def silence_format(self, silence):
        """
        格式化silence
        :param silence:
        :type silence: dict
        :rtype: str
        """
        if silence.get("status", {}).get("state") != "expired":
            return f"SilenceID: {silence.get('id')}, CreatedBy: {silence.get('createdBy')}, StartsAt: {silence.get('startsAt')}, EndsAt: {silence.get('endsAt')}, Comment: {silence.get('comment')}"
        else:
            return ""

    def delete_silence(self, silence_id):
        """
        删除silence
        :param silence_id:
        :type silence_id: str
        :rtype: None
        """
        response = requests.delete(urljoin(self.url, "api/v2/silence/{}".format(silence_id)), headers=self.__headers(),
                                   timeout=self.timeout)
        if response.status_code != 200:
            raise SilenceError(response.text)

    def delete_silences(self):
        """
        删除所有silence
        :rtype: None
        """
        for silence in self.list_silences():
            self.delete_silence(silence.get("id"))