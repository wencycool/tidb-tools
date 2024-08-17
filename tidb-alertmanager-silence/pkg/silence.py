# coding=utf8
import requests
from datetime import datetime, timedelta
import pytz
import json
from urllib.parse import urljoin

# 创建silence类型，目前支持整个集群的silence或者某一个组件的silence，即某一个组件如果宕机后不再发送告警


# 创建silence Exception
class SilenceError(Exception):
    def __init__(self, message):
        self.message = message


def local2utc(local_st):
    """
    将本地时间转换为UTC时间
    :param local_st:
    :type local_st: datetime
    :rtype: datetime
    """
    local_timezone = pytz.timezone("Asia/Shanghai")
    local_dt = local_timezone.localize(local_st)
    utc_dt = local_dt.astimezone(pytz.utc)
    return utc_dt


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

    def __generate_data(self, alertname, startsAt=datetime.now(),
                        endsAt=datetime.now() + timedelta(minutes=30), createdBy='system',
                        comment='auto silence'):
        """
        生成data对象
        :param alertname:
        :type alertname: str
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
        if alertname is None or len(alertname.strip()) == 0:
            data = {
                "matchers": [
                    {
                        "name": "alertname",
                        "value": ".+",
                        "isRegex": True,
                    }
                ],
                "startsAt": startsAt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "endsAt": endsAt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "createdBy": createdBy,
                "comment": comment,
            }
        else:
            data = {
                "matchers": [
                    {
                        "name": "alertname",
                        "value": alertname,
                        "isRegex": True,
                    }
                ],
                "startsAt": startsAt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "endsAt": endsAt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "createdBy": createdBy,
                "comment": comment,
            }
        return json.dumps(data)

    def create_silence(self, alertname, startsAt, endsAt):
        """
        创建silence
        :param alertname:
        :type alertname: list(str)
        :param startsAt:
        :type startsAt: datetime
        :param endsAt:
        :type endsAt: datetime
        :rtype: dict
        """
        data = self.__generate_data(alertname, startsAt, endsAt)
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