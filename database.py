import mysql.connector

from configs import dbconfig

import sys

print('Running MySQL Client...')

connection = mysql.connector.connect(
    host=dbconfig.db_host,
    port=dbconfig.db_port,
    user=dbconfig.db_username,
    password=dbconfig.db_password,
    database=dbconfig.db_name,
    charset=dbconfig.db_charset,
    collation=dbconfig.db_collation,
    auth_plugin='mysql_native_password',
)

cursor = None

if connection.is_connected():
    print("Connected to MySQL Server version", connection.get_server_info())
    cursor = connection.cursor(buffered=True)

def getConnection():
    return connection

def getCursor():
    return cursor