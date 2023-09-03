import configparser
import os
print(os.getcwd())

VERSION = '0.0.1'

BOT_INI_PATH = './configs/bot_docker.ini'

config = configparser.ConfigParser()
config.read(BOT_INI_PATH)

DB_PASSWORD = config['db']['password']
DB_NAME = config['db']['database']
DB_HOST = config['db']['host']
DB_USER = config['db']['user']
DB_PORT = config['db']['port']

BOT_TOKEN = config['random_coffee_bot']['token']

ALARM_BOT_TOKEN = config['alarm_bot']['token']

CHAT_ID_ALARM = config['alarm_bot']['chat_id']
CHAT_ID_ALARM_WITH_HR = config['alarm_bot']['chat_id_with_HR']

REDIS_HOST = config['redis']['redis_host']
REDIS_PORT = config['redis']['redis_port']
REDIS_DP = config['redis']['redis_dp']
REDIS_PASSWORD = config['redis']['redis_password']
