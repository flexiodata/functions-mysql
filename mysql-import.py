
# ---
# name: mysql-import
# deployed: true
# title: MySQL Import
# description: Creates pipes to access MySQL tables
# ---

import base64
import mysql.connector
from mysql.connector import FieldType
from datetime import *
from decimal import *

# main function entry point
def flex_handler(flex):
    create_functions(flex)

def create_functions(flex):

    # get the parameters
    params = dict(flex.vars)
    files = params['files']
    connection_info = params['mysql-connection']

    # connect to mysql
    config = {
      'host': connection_info['host'],
      'user': connection_info['username'],
      'password': connection_info['password'],
      'database': connection_info['database'],
      'raise_on_warnings': False
    }
    connection = mysql.connector.connect(**config)

    # create functions for each of the selected tables
    for f in files:

        file_parts = f.split(':/')
        file_name = file_parts[1]

        function_info = get_function_info(connection, file_name)
        flex.index.remove(function_info['name'])
        flex.index.create(function_info['name'], function_info)

def to_date(value):
    return value

def to_string(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (Decimal)):
        return str(value)
    return value

def get_function_info(connection, table):

    # get the table structure
    cursor = connection.cursor()
    query = "select * from " + table + " limit %(count)s"
    cursor.execute(query, {"count": 1})
    cursor.fetchall()

    column_info = []
    for desc in cursor.description:
        info = {}
        info['name'] = desc[0]
        info['type'] = FieldType.get_info(desc[1]) # TODO: convert mysql type to json type
        column_info.append(info)

    cursor.close()

    # return the function info
    info = {}
    info['name'] = 'mysql-' + table.lower() # TODO: make clean name
    info['title'] = ''
    info['description'] = ''
    info['task'] = {
        'op': 'sequence',
        'items': [{
            'op': 'execute',
            'lang': 'python',
            'code': get_function_extract_task(table)
        }]
    }
    info['returns'] = column_info
    info['run_mode'] = 'P'
    info['deploy_mode'] = 'R'
    info['deploy_api'] = 'A'

    return info

def get_function_extract_task(table):
    code = """

import json
import mysql.connector
from datetime import *
from decimal import *

# main function entry point
def flex_handler(flex):

    # get the parameters
    params = dict(flex.vars)
    files = params['files']
    connection_info = params['mysql-connection']

    # connect to mysql
    config = {
      'host': connection_info['host'],
      'user': connection_info['username'],
      'password': connection_info['password'],
      'database': connection_info['database'],
      'raise_on_warnings': False
    }
    connection = mysql.connector.connect(**config)
    cursor = connection.cursor()

    query = "select * from """ + table + """ limit 5"
    cursor.execute(query)

    rows = []
    rows += [cursor.column_names]
    rows += cursor.fetchall()

    cursor.close()
    connection.close()

    result = json.dumps(rows, default=to_string)
    flex.output.content_type = "application/json";
    flex.output.write(result)

def to_string(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (Decimal)):
        return str(value)
    return value

"""
    code = code.encode('utf-8')
    return base64.b64encode(code).decode('utf-8')

