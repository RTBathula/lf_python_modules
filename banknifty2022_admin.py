import os
import psycopg2
from psycopg2 import sql, Error

import postgres_util

banknifty2022_conn = None

def db_instance():
    global banknifty2022_conn

    if banknifty2022_conn is None:      
        params = {
            "host": os.getenv('POSTGRES_HOST'),
            "port": os.getenv('POSTGRES_PORT'),
            "user": os.getenv('POSTGRES_USER'),
            "password": os.getenv('POSTGRES_PASSWORD'),       
            "database": os.getenv('POSTGRES_DATABASE')
        }

        banknifty2022_conn = psycopg2.connect(**params) 

    return banknifty2022_conn


def ensure_expiry_date_table(conn, schema, date):
    table = sql.SQL("""
        create table if not exists {}.{}(
            id serial primary key,
            datetime timestamptz,
            option_type varchar(2),
            strike_price integer,        
            symbol_expiry varchar(50),
            open numeric(18, 2),
            high numeric(18, 2),
            low numeric(18, 2),
            close numeric(18, 2),
            volume integer,
            open_interest integer
        )
    """).format(sql.Identifier(schema), sql.Identifier(date))

    try:
        cursor = conn.cursor()
        cursor.execute(table)
        conn.commit()
        cursor.close()
        return {"data": "successfully created!", "error": None}
    except Exception as e:
        cursor.close()
        return {"data": None, "error": e}

def ensure_schema(schema):
    conn = db_instance()    
    return postgres_util.create_schema(conn, schema)

def ensure_schema_table(schema, date):
    conn = db_instance()    
    schema_resp = postgres_util.create_schema(conn, schema)

    if schema_resp["error"] is not None:        
        return schema_resp

    return ensure_expiry_date_table(conn, schema, date)
    


    

