import re

opt_exipiry_regex = re.compile(r'(\d{6})')
opt_type_regex = re.compile(r'(PE|CE)')

year_prefix = "20"

def break_option_symbol(option):
	result = re.split(opt_exipiry_regex, option)
	type_result = re.split(opt_type_regex, result[2])

	return {
		"option": result[0],
		"expiry": result[1],
		"strike_price": int(type_result[0]),
		"option_type": type_result[1]
	}