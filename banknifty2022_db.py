import os
import json
import psycopg2
from psycopg2 import sql, Error
from psycopg2.extras import RealDictCursor

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

def list_expiries():
    conn = db_instance()
    query = sql.SQL("""
        select schema_name as expiry from information_schema.schemata 
        where schema_name like 'OPT%' order by schema_name
    """)

    return postgres_util.get_list(conn, query)

def list_days_by_expiry(expiry):
    conn = db_instance()
    query = sql.SQL("""
        select table_name as day from information_schema.tables
        where table_schema = %s order by table_name
    """)

    return postgres_util.get_list(conn, query, [expiry])

def list_days_options(expiry, expiry_day, query_time):
    conn = db_instance()
    query = sql.SQL("""
        select * from {}.{} where 
            datetime = %s
        order by option_type                
    """).format(sql.Identifier(expiry), sql.Identifier(expiry_day))

    values = [
        query_time
    ]

    return postgres_util.get_list(conn, query, values)

def find_option_by_time_price(
    expiry, 
    expiry_day,
    min_datetime,
    max_datetime,
    min_close_price,
    max_close_price,
    option_type
):
    conn = db_instance()
    query = sql.SQL("""
        select * from {}.{} where 
            datetime >= %s 
            and datetime <= %s
            and close >= %s
            and close <= %s
            and option_type = %s            
    """).format(sql.Identifier(expiry), sql.Identifier(expiry_day))

    values = [
        min_datetime, 
        max_datetime, 
        min_close_price, 
        max_close_price,
        option_type
    ]

    return postgres_util.find_one(conn, query, values)

def find_option_by_time_strike_price(
    expiry, 
    expiry_day,
    datetime_query,
    strike_price,    
    option_type
):
    conn = db_instance()

    query_string = """
        select * from {}.{} where 
            datetime = %s           
            and strike_price = %s                      
    """

    values = [
        datetime_query,
        strike_price            
    ]

    if option_type is not None:
        query_string = """
            select * from {}.{} where 
                datetime = %s           
                and strike_price = %s            
                and option_type = %s            
        """

        values.append(option_type)

    query = sql.SQL(query_string).format(sql.Identifier(expiry), sql.Identifier(expiry_day))
    return postgres_util.get_list(conn, query, values)
    
def find_option_by_time_premium(
    expiry, 
    expiry_day,
    datetime_query,
    premium_min,
    premium_max,   
    option_type
):
    conn = db_instance()
    query = sql.SQL("""
        select * from {}.{} where 
            datetime = %s           
            and close >= %s 
            and close <= %s             
            and option_type = %s            
    """).format(sql.Identifier(expiry), sql.Identifier(expiry_day))

    values = [
        datetime_query,
        premium_min,
        premium_max,   
        option_type       
    ]

    return postgres_util.find_one(conn, query, values)

