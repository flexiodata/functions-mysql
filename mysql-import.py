
# ---
# name: mysql-import
# deployed: true
# title: MySQL Import
# description: Creates pipes to access MySQL tables
# ---


# main function entry point
def flex_handler(flex):

    get_data(flex)

def get_data(flex):
    pass

def to_date(value):
    return value

def to_string(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (Decimal)):
        return str(value)
    return value

