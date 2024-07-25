import yaml
import pathlib


# 设置alert规则的Exception类
class AlertRuleNotFoundException(Exception):
    # 打印异常信息
    def __str__(self):
        return 'Alert rule:[%s] not found.' % self.args[0]


# 设置PrometheusRulesManager无法解析的Exception类

class PrometheusRulesManagerException(Exception):
    # 打印异常信息
    def __str__(self):
        return 'PrometheusRulesManagerException: %s' % self.args[0]


class PrometheusRulesManager:
    """
    PrometheusRulesManager类用于管理Prometheus的规则文件，可以删除和修改alert规则
    注意：通过该类修改的规则文件会丢失原有的注释信息
    """

    def __init__(self, file_path):
        self.file_path = file_path
        self.rules = self.load_rules()

    def is_rulefile(self):
        """
        要求文件名必须在特定的列表中
        :rtype:bool
        """
        base_path = pathlib.Path(self.file_path).name
        return base_path in ['tidb.rules.yml', 'tikv.rules.yml', 'pd.rules.yml', 'tikv.accelerate.rules.yml',
                             'binlog.rules.yml', 'ticdc.rules.yml', 'tiflash.rules.yml', 'lightning.rules.yml',
                             'blacker.rules.yml', 'node.rules.yml', 'kafka.rules.yml']

    def load_rules(self):
        try:
            with open(self.file_path, 'r', encoding='utf-8') as file:
                rules = yaml.safe_load(file)
                if not rules or 'groups' not in rules or not rules['groups'] or 'rules' not in rules['groups'][0] or not \
                rules['groups'][0]['rules']:
                    raise PrometheusRulesManagerException('Invalid rules file')
                return rules
        except FileNotFoundError:
            raise Exception(f'File {self.file_path} does not exist.')
        except yaml.YAMLError as e:
            raise Exception(f'Error loading YAML file: {e}')

    def save_rules(self):
        try:
            with open(self.file_path, 'w', encoding='utf-8') as file:
                yaml.dump(self.rules, file, allow_unicode=True, default_flow_style=False, sort_keys=False, width=1000)
        except yaml.YAMLError as e:
            raise Exception(f'Error saving YAML file: {e}')

    def find_alert_index(self, alert_name):
        """
        根据alert_name查找对应的alert规则的索引
        :param alert_name: alert的名称
        :type alert_name: str
        :return: alert规则的索引
        :rtype: int
        """
        for rule_index, rule in enumerate(self.rules['groups'][0]['rules']):
            if rule.get('alert') == alert_name:
                return rule_index
        raise AlertRuleNotFoundException(alert_name)

    def append_alert_rules(self, file_path, after=None):
        """
        追加一个或多个alert规则到原有的规则文件中
        :param file_path: 要追加的规则文件的路径
        :type file_path: str
        :param after: 追加在哪个alert规则之后，如果不指定则追加在最后
        :type after: str
        """
        with open(file_path, 'r', encoding='utf-8') as file:
            new_rules = yaml.safe_load(file)
            # 校验new_rules中的所有规则名称不应该在原有的规则文件中存在
            try:
                rules = new_rules['groups'][0]['rules']
            except (KeyError, TypeError):
                raise PrometheusRulesManagerException('Invalid rules file')
            for rule in rules:
                try:
                    self.find_alert_index(rule['alert'])
                    raise Exception(f'Alert rule {rule["alert"]} already exists in the original rules file')
                except AlertRuleNotFoundException:
                    pass
            if after:
                rule_index = self.find_alert_index(after) + 1
                self.rules['groups'][0]['rules'][rule_index:rule_index] = new_rules['groups'][0]['rules']
            else:
                self.rules['groups'][0]['rules'].extend(new_rules['groups'][0]['rules'])

    def delete_alert_rule(self, alert_name):
        rule_index = self.find_alert_index(alert_name)
        del self.rules['groups'][0]['rules'][rule_index]

    def modify_alert_rule(self, alert_name, modifications):
        """
        根据指定的alert_name修改对应的属性值，modifications是一个字典，key是属性名，value是属性值，key支持多级属性，使用.分隔，例如：
        modification = {'for': '10m', 'annotations.summary': 'xxx'},则会将alert_name对应的规则的for属性值修改为10m，annotations的summary属性值修改为xxx
        :param alert_name: alert的名称
        :type alert_name: str
        :param modifications: 修改的属性值，字典类型，可以修改多个值
        :type modifications: dict
        """
        rule_index = self.find_alert_index(alert_name)
        keys = modifications.keys()
        try:
            for key in keys:
                keys_list = key.split('.')
                rule = self.rules['groups'][0]['rules'][rule_index]
                for k in keys_list[:-1]:
                    rule = rule[k]
                # 判断keys_list[-1]是否在rule中，如果不在则抛出异常
                if keys_list[-1] not in rule:
                    raise Exception(f'Key {keys_list[-1]} not found in alert rule {alert_name}')
                rule[keys_list[-1]] = modifications[key]
        except KeyError:
            raise Exception(f'Key {key} not found in alert rule {alert_name}')
