import json
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

def check_schema_exist(conn, schema):
	query = sql.SQL("select schema_name from information_schema.schemata where schema_name = %s")

	try:
		cursor = conn.cursor()
		cursor.execute(query, [schema])
		records = cursor.fetchall()	
		cursor.close()		
		
		return {"data": len(records) > 0, "error": None} 
	except Exception as e:
		cursor.close()
		return {"data": None, "error": e}


def create_schema(conn, schema):
	query = sql.SQL("create schema if not exists {}").format(sql.Identifier(schema))

	try:
		cursor = conn.cursor()
		cursor.execute(query)
		conn.commit()
		cursor.close()
		return {"data": "successfully created!", "error": None}
	except Exception as e:
		cursor.close()
		return {"data": None, "error": e}

def get_list(conn, query, param_list=None):
	try:
		cursor = conn.cursor(cursor_factory=RealDictCursor)
		cursor.execute(query, param_list)
		records = cursor.fetchall()     
		records =  json.loads(json.dumps(records, indent=4, sort_keys=True, default=str))
		cursor.close()      

		return {"data": records, "error": None} 
	except Exception as e:
		cursor.close()
		return {"data": None, "error": e}

def find_one(conn, query, param_list=None):
	try:
		cursor = conn.cursor(cursor_factory=RealDictCursor)
		cursor.execute(query, param_list)
		record = cursor.fetchone()
		record =  json.loads(json.dumps(record, indent=4, sort_keys=True, default=str))
		cursor.close()

		return {"data": record, "error": None} 
	except Exception as e:
		cursor.close()
		return {"data": None, "error": e}

