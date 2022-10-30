from datetime import datetime, timedelta

def str_zulu_to_iso(time_input):
	naive_str = str(time_input).replace('Z', '.00+00:00')
	return str_to_iso(naive_str)

def str_to_iso(time_input):	
	return datetime.strptime(time_input, "%Y-%m-%dT%H:%M:%S.%f%z")

def py_str_to_iso(time_input):	
	return datetime.strptime(time_input, "%Y-%m-%d %H:%M:%S%z")

def obj_to_iso(time_obj):	
	return time_obj.strftime("%Y-%m-%dT%H:%M:%S.%f%z")

def obj_to_str(time_input):
	return str(time_input)

def increment_mins(time_obj, increment_by):
	return time_obj + timedelta(minutes=increment_by)