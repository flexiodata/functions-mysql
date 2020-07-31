
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

        file_name = f.get('name')
        if file_name is None:
            continue

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

    # get the column info; see here for field type list:
    # http://mysql-python.sourceforge.net/MySQLdb-1.2.2/public/MySQLdb.constants.FIELD_TYPE-module.html
    column_info = []
    for desc in cursor.description:

        func_type = 'string'
        db_type = FieldType.get_info(desc[1])

        if db_type in ['BIT', 'INT24', 'SHORT', 'LONG']:
            func_type = 'integer'
        if db_type in ['DOUBLE', 'FLOAT']:
            func_type = 'number'

        info = {}
        info['name'] = desc[0]
        info['type'] =  func_type
        info['description'] = 'The ' + info['name'] + ' for the item'
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
import urllib
import itertools
import mysql.connector
from datetime import *
from decimal import *
from cerberus import Validator
from collections import OrderedDict

# main function entry point
def flex_handler(flex):

    # get the input
    input = flex.input.read()
    input = json.loads(input)
    if not isinstance(input, list):
        input = []

    # define the expected parameters and map the values to the parameter names
    # based on the positions of the keys/values
    params = OrderedDict()
    params['properties'] = {'required': False, 'validator': validator_list, 'coerce': to_list, 'default': '*'}
    params['filter'] = {'required': False, 'type': 'string', 'default': ''}
    params['config'] = {'required': False, 'type': 'string', 'default': ''} # index-styled config string
    input = dict(zip(params.keys(), input))

    # validate the mapped input against the validator
    v = Validator(params, allow_unknown = True)
    input = v.validated(input)
    if input is None:
        raise ValueError

    # get the properties to return and the property map;
    # if we have a wildcard, get all the properties
    properties = [p.strip() for p in input['properties']]
    if len(properties) == 1 and (properties[0] == '' or properties[0] == '*'):
        properties = ['*']

    # get the filter
    filter = input['filter']
    if len(filter) == 0:
        filter = 'True'

    # get any configuration settings
    config = urllib.parse.parse_qs(input['config'])
    config = {k: v[0] for k, v in config.items()}
    limit = int(config.get('limit', 100))
    headers = config.get('headers', 'true').lower()
    if headers == 'true':
        headers = True
    else:
        headers = False

    # get the connection info
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

    query = \"\"\"
        select {columns} from {table} where {filter} limit {limit}
    \"\"\".format(
        table = '""" + table + """',
        columns = ','.join(properties),
        filter = filter,
        limit = limit
    )
    cursor.execute(query)

    rows = []
    if headers is True:
        rows += [cursor.column_names]
    rows += cursor.fetchall()

    cursor.close()
    connection.close()

    result = json.dumps(rows, default=to_string)
    flex.output.content_type = "application/json"
    flex.output.write(result)

def to_string(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (Decimal)):
        return str(value)
    return value

def validator_list(field, value, error):
    if isinstance(value, str):
        return
    if isinstance(value, list):
        for item in value:
            if not isinstance(item, str):
                error(field, 'Must be a list with only string values')
        return
    error(field, 'Must be a string or a list of strings')

def to_list(value):
    # if we have a list of strings, create a list from them; if we have
    # a list of lists, flatten it into a single list of strings
    if isinstance(value, str):
        return value.split(",")
    if isinstance(value, list):
        return list(itertools.chain.from_iterable(value))
    return None

"""
    code = code.encode('utf-8')
    return base64.b64encode(code).decode('utf-8')

