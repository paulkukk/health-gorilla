import os
from io import StringIO

import psycopg2
import psycopg2.extras as extras

DEFAULT_SCHEMA = "salesforce"


def execute_sql(conn, sql):
    cursor = conn.cursor()
    try:
        cursor.execute(sql)

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
    finally:
        conn.commit()
        cursor.close()


def update_last_time_run(conn, update_time, job_name):
    sql = f"""UPDATE config.job SET last_ran = '{update_time}' WHERE name = '{job_name}'"""
    cursor = conn.cursor()
    try:
        cursor.execute(sql)

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
    finally:
        conn.commit()
        cursor.close()


def get_last_time_run(conn, job_name):
    date_value = ''
    sql = f"""SELECT last_ran FROM config.job WHERE name = '{job_name}'"""
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        record = cursor.fetchone()
        date_value = record[0]

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
    finally:
        cursor.close()

    return date_value


def execute_many(conn, df, table):
    """
    Using cursor.executemany() to insert the dataframe
    """
    # Create a list of tuples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))

    # SQL query to execute
    # query = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s)" % (table, cols)
    values = '%%s'
    tuples_cnt = len(df.columns) - 1
    print('COUNT', tuples_cnt)
    if tuples_cnt > 0:
        for i in range(tuples_cnt):
            values = values + ',%%s'
    else:
        values = '%%s'
    sql = "INSERT INTO %s(%s) VALUES(" + values + ")"

    query = sql % (table, cols)
    # print(tuples)
    # print(query)
    cursor = conn.cursor()
    try:
        cursor.executemany(query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_many() done")
    cursor.close()


def execute_batch_list(conn, df, table, constraint_columns: list = [], page_size=100):
    '''
    columns = p_list[0].keys()
    print(columns)
    query = "INSERT INTO " + table + " ({}) VALUES (%s)".format(','.join(columns))
    values = [[value for value in project.values()] for project in p_list]
    '''
    print('Upserting ', len(df.index), ' rows.')
    index = list(df.index.names)
    tuples = [tuple(x) for x in df.to_numpy()]
    columns = list(df.columns)
    # Building the top portion of the SQL statement of the clumns to be inserted on
    head_columns = ", ".join([f'{col.lower()}' for col in columns])
    # headers = columns

    update_column_stmt = ", ".join([f'"{col.lower()}" = EXCLUDED."{col.lower()}"' for col in columns])
    values = '%s'
    tuples_cnt = len(df.columns) - 1
    print('COUNT', tuples_cnt)
    if tuples_cnt > 0:
        for i in range(tuples_cnt):
            values = values + ',%s'
    else:
        values = '%s'
    # Compose and execute upsert query
    query_upsert = f"""
        INSERT INTO {table} ({head_columns}) 
        VALUES ({values})
        ON CONFLICT ({constraint_columns[0]}) DO UPDATE 
        SET {update_column_stmt};
        """

    cursor = conn.cursor()

    try:
        # print(tuples)
        # execute_values(cursor, query, values)
        extras.execute_batch(cursor, query_upsert, tuples, page_size)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_batch() done")


def execute_batch_test(conn, df, table, page_size=100):
    """
    Using psycopg2.extras.execute_batch() to insert the dataframe
    """
    # Create a list of tuples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))

    # SQL query to execute
    # query = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s)" % (table, cols)
    values = '%%s'
    tuples_cnt = len(df.columns) - 1
    print('COUNT', tuples_cnt)
    if tuples_cnt > 0:
        for i in range(tuples_cnt):
            values = values + ',%%s'
    else:
        values = '%%s'
    sql = "INSERT INTO %s(%s) VALUES(" + values + ")"

    query = sql % (table, cols)
    # query = "INSERT INTO %s(%s) VALUES(%%s)" % (table, cols)
    print(query)
    cursor = conn.cursor()
    try:
        # print(tuples)
        extras.execute_batch(cursor, query, tuples, page_size)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_batch() done")
    cursor.close()


def execute_batch(conn, df, table, page_size=100):
    """
    Using psycopg2.extras.execute_batch() to insert the dataframe
    """
    # Create a list of tuples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query = "INSERT INTO %s(%s) VALUES(%%s)" % (table, cols)
    print(query)
    cursor = conn.cursor()
    try:
        print(tuples)
        extras.execute_batch(cursor, query, tuples, page_size)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_batch() done")
    cursor.close()


def execute_values(conn, df, table):
    """
    Using psycopg2.extras.execute_values() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()


def execute_mogrify(conn, df, table):
    """
    Using cursor.mogrify() to build the bulk insert query
    then cursor.execute() to execute the query
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    cursor = conn.cursor()
    values = [cursor.mogrify("(%s,%s,%s)", tup).decode('utf8') for tup in tuples]
    query = "INSERT INTO %s(%s) VALUES " % (table, cols) + ",".join(values)

    try:
        cursor.execute(query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_mogrify() done")
    cursor.close()


def copy_from_file(conn, df, table):
    """
    Here we are going save the dataframe on disk as
    a csv file, load the csv file
    and use copy_from() to copy it to the table
    """
    # Save the dataframe to disk
    tmp_df = "./tmp_dataframe.csv"
    df.to_csv(tmp_df, index_label='id', header=False)
    f = open(tmp_df, 'r')
    cursor = conn.cursor()
    try:
        cursor.copy_from(f, table, sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        os.remove(tmp_df)
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("copy_from_file() done")
    cursor.close()
    os.remove(tmp_df)


def copy_from_stringio(conn, df, table):
    """
    Here we are going save the dataframe in memory
    and use copy_from() to copy it to the table
    """
    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, index_label='id', header=False)
    buffer.seek(0)

    cursor = conn.cursor()
    try:
        cursor.copy_from(buffer, table, sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("copy_from_stringio() done")
    cursor.close()
