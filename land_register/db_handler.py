import mysql.connector as mariadb


def get_connection():
    return mariadb.connect(
        # host='katastr-db.csnbslf6zcko.eu-central-1.rds.amazonaws.com',
        # user='devmons',
        # password='NG1MMUGuZBgT7rxvnpYq',
        user='user',
        password='password',
        database='katastr_db')


def insert_or_update(query, values=None):
    id = None
    try:
        connection = get_connection()
        cursor = connection.cursor()
        if values:
            cursor.execute(query, values)
        else:
            cursor.execute(query)
        connection.commit()

        id = cursor.lastrowid
    except Exception as e:
         print(e)
    finally:
        cursor.close()
        connection.close()

    return id


def load(query, values=None, single_item=False, with_col_names=True):
    result = None
    try:
        connection = get_connection()
        cursor = connection.cursor(buffered=True)

        cursor.execute(query, values)

        result = cursor.fetchall()
        if with_col_names:
            columns = cursor.description
            result = [{columns[index][0]:column for index, column
                in enumerate(value)} for value in result]

        connection.commit()
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        connection.close()

    if result:
        return result[0] if single_item else result
    return result
