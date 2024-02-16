from configparser import ConfigParser
from snowflake.connector import connect


class Connection:
    def __init__(self, path=None):
        self.config = ConfigParser()
        self.config.read(path)

    def get_config_details(self):
        environment: dict = {}
        sections: list = self.config.sections()
        for section in sections:
            items = self.config.items(section)
            environment[section] = dict(items)
        return environment

    def connect(self):
        connection_parameter = self.get_config_details()['DATABASE']
        return connect(**connection_parameter)
