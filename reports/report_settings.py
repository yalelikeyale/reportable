import os
import json
import time
import MySQLdb
import math
import pandas as pd
import pandas.io.sql as psql
from ftplib import FTP
from datetime import date, datetime, time, timedelta
from collections import OrderedDict
from reports import config

INPUT_FILE = "sid_uid.csv"

mysqlcon = config.mysqlcon
ftp_creds = config.ftp_creds

ftp_host = ftp_creds['host']
ftp_port = ftp_creds['port']
ftp_path = ftp_creds['path']
ftp_user = ftp_creds['username']
ftp_pass = ftp_creds['password']

con = MySQLdb.connect(host=mysqlcon['host'] , port=mysqlcon['port'], user=mysqlcon['username'], 
		              passwd=mysqlcon['password'], db=mysqlcon['database'] )

# Returns user id of associated store
def get_user(store_id):
	get_user_query = '''select user_id
		from stores
		where id = %s;'''
	user_id = pd.read_sql(get_user_query % (store_id), con = con).loc[0]["user_id"]
	return user_id

# Return true iff the user's ftp directory exists
def check_ftp_user(uid):
	ftp = FTP()
	ftp.connect(ftp_creds['host'], ftp_creds['port'])
	ftp.login(ftp_creds['username'], ftp_creds['password'])
	print "logged in...checking user dir"
	try:
		ftp.cwd(ftp_creds['path'] % ("/User%s" % uid))
	except Exception, e:
		print e
		return False
	else:
		return True

def get_schedule(gmt_timezone, schedule_at):
	if schedule_at and schedule_at !=0 and not pd.isnull(schedule_at):
		try:
			gmt_timezone = int(gmt_timezone)
			schedule_at = datetime.strptime(schedule_at,("%H:%M")) #.strftime("%Y-%m-%d %H:%M:%S")
			schedule_at = datetime.combine(datetime.date(datetime.today()),datetime.time(schedule_at))
			schedule_at = (schedule_at - timedelta(hours=int(gmt_timezone)))
		except Exception, e:
			print e
			return e, -1
		else:
			print "Schedule At: ", schedule_at
			return gmt_timezone, schedule_at
	else:
		schedule_at = datetime.strptime(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),("%Y-%m-%d %H:%M:%S"))
		gmt_timezone = -8
		print "No schedule given, setting for now()"
		return gmt_timezone, schedule_at

def get_store_type(store_id):
	is_amazon = 0
	is_ebay = 0
	store_type_query = '''select a.is_visual_store, t.name as store_type, s.ftp_mapping, s.custom_columns
							from stores as s
							join account_settings as a on a.store_id = s.id
							join store_types as t on a.type = t.id
							where s.id =%s;'''
	store_settings = pd.read_sql(store_type_query % store_id, con = con)
	store_type = store_settings.loc[0]["store_type"]
	ftp_mapping = store_settings.loc[0]["ftp_mapping"]
	if store_type == "Amazon":
		is_amazon = 1
	if store_type == "eBay":
		is_ebay = 1
	print "store type: ", store_type
	print "custom ftp mapping: ", ftp_mapping
	return store_type, is_amazon, is_ebay, ftp_mapping

def get_product_fields(store_id, is_ebay):
	msrp_query = '''select count(pps.msrp) as msrp_count
			from products_per_store as pps
			where pps.store_id = %s and pps.msrp is not null and pps.msrp > 0;'''

	msrp_result = pd.read_sql(msrp_query % store_id, con = con)
	msrp = msrp_result.loc[0]["msrp_count"]

	condition_query = '''select count(pps.condition) as cond_count
			from products_per_store as pps
			where pps.store_id = %s and pps.condition is not null and pps.condition not like "new";'''

	condition_result = pd.read_sql(condition_query % store_id, con = con)
	cond = condition_result.loc[0]["cond_count"]

	product_fields = OrderedDict([
                ("name", "Product Name"),
                ("sku", "Inventory Number"),
                ("stock_level", "IN STOCK"),
                ("upc", "UPC/EAN"),
                ("asin", "ASIN"),
                ("epid", "ePid"),
                ("brand", "Make"),
                ("model", "Model"),
                ("mpn", "MPN"),
                ("condition", "Condition"),
                ("store_price", "Product Price"),
                ("min_price", "Minimum Price"),
                ("max_price", "Maximum Price"),
                ("store_cost", "Cost"),
                ("store_ship", "Shipping Price"),
                ("product_url", "Product URL"),
                ("image_url", "Image URL"),
                ("wiseprice", "New Price"),
                ("keyword", "Labels"),
                ("msrp", "MSRP")
            ])
	if msrp == 0:
		product_fields.popitem(last=True) # do NOT include msrp
	if cond == 0:
		del product_fields["condition"]
	if is_ebay != 1:
		del product_fields["epid"]


	print "msrp: ", msrp
	print "condition: ", cond
	return product_fields

def get_custom_columns(store_id):
	custom_column_names = []
	custom_columns_query = '''select att_name
		from pps_custom_attributes
		where fk_store_id = %s
		group by att_name;'''
	custom_column_results = pd.read_sql(custom_columns_query % store_id, con = con)
	custom_column_names = list(custom_column_results["att_name"])
	print "custom column names: ", custom_column_names
	custom_attr_fields = {}
	for field_name in custom_column_names:
		custom_attr_fields[field_name] = field_name
	custom_attr_fields = OrderedDict(sorted(custom_attr_fields.items()))
	return custom_attr_fields

def get_prefs(store_id):
	uid = get_user(store_id)
	comp_url = 0
	competitor_per_row = 0
	max_comps = 5
	store_prefs_query = '''select *
		from
		(select *
		from 
		((select pt.category, pt.description, pd.name, pd.uid, pd.store_id, pd.val
		from preferences_data as pd
		join preferences_types as pt on pd.name = pt.name
		where (pd.store_id = 1179059371 or pd.uid = 5960))
		UNION
		(select p.category, p.description, p.name, 0 as uid, 0 as store_id, p.def_val as val
		from preferences_types as p)) b
		where b.category LIKE "csv_export" or b.name in ("IS_UOM_STORE", "WM_CSV_EXPORT_SS")
		order by b.name, b.store_id DESC, b.uid DESC) a
		group by a.name
		order by a.category, a.name, a.store_id DESC, a.uid DESC;'''
	store_prefs = pd.read_sql(store_prefs_query % (store_id, uid), con = con)

	### Parsing Prefs ###
	map_screenshots = store_prefs[store_prefs["name"] == "WM_CSV_EXPORT_SS"]['val'].values[0]
	if not map_screenshots:
		map_screenshots = 0
	print "map ss: ", map_screenshots
	price_includes_shipping = store_prefs[store_prefs["name"] == "FTP_EXPORT_COMPETITORS_PRICE_SHIPPING"]['val'].values[0]
	if not price_includes_shipping:
			price_includes_shipping = 0
	print "price includes shipping: ", price_includes_shipping
	is_uom = store_prefs[store_prefs["name"] == "IS_UOM_STORE"]['val'].values[0]
	if not is_uom:
		is_uom = 0
	print "is UOM: ", is_uom
	csv_delimiter = store_prefs[store_prefs["name"] == "CSV_FILE_EXPORT_DELIMITER"]['val'].values[0]
	if not csv_delimiter:
		csv_delimiter = ','
	print "csv_delimiter: ", csv_delimiter
	max_comps = store_prefs[store_prefs["name"] == "CSV_EXPORT_COMP_COUNT"]['val'].values[0]
	if not max_comps:
		max_comps = 5
	print "max_comps: ", max_comps
	comp_url = store_prefs[store_prefs["name"] == "CSV_EXPORT_FULL_URL"]['val'].values[0]
	if not comp_url:
		comp_url = 0
	print "comp_url: ", comp_url
	competitor_per_row = store_prefs[store_prefs["name"] == "CSV_EXPORT_ONE_ROW_ONE_COMP"]['val'].values[0]
	if competitor_per_row is None or competitor_per_row == '':
		competitor_per_row = 0
	print "competitor_per_row: ", competitor_per_row
	## Building comp data objects
	if int(competitor_per_row) > 0:
		competitor_store_data_fields = {
		        "store_name": "Competitor"
		    }
		competitor_fields = OrderedDict([
		        ("price", "Competitor Price"),
		        ("ship", "Competitor Shipping"),
		        ("url", "Competitor URL")
		    ])
	else:
		competitor_store_data_fields = {
		        "store_name": "Comp${NUM}"
		    }
		competitor_fields = OrderedDict([
		            ("price", "Comp${NUM} Price"),
		            ("ship", "Comp${NUM} Shipping"),
		            ("url", "Comp${NUM} URL")
		        ])
	if comp_url == 0:
			competitor_fields.popitem(last=True) # do NOT include competitor url

	# old unaccepted prefs
	only_new_price = store_prefs[store_prefs["name"] == "FTP_EXPORT_ONLY_NEW_PRICE"]['val'].values[0]
	only_valid_wiseprice = store_prefs[store_prefs["name"] == "FTP_EXPORT_ONLY_VALID_WISEPRICE"]['val'].values[0]

	return {'user_id': uid, 'comp_url': comp_url,'map_screenshots': map_screenshots, 'price_includes_shipping': price_includes_shipping, 'is_uom': is_uom, 'csv_delimiter': csv_delimiter, 'competitor_per_row': competitor_per_row, 'max_comps': max_comps, 'competitor_store_data_fields': competitor_store_data_fields, 'competitor_fields': competitor_fields, 'only_new_price': only_new_price, 'only_valid_wiseprice': only_valid_wiseprice}



## generate report data based on prefs from store

def generate_report_data(store_id, gmt_timezone, schedule_at, include_timestamp):
	# get user id
	user_id = get_user(store_id)

	print "Checking User's FTP Directory"
	check_ftp = check_ftp_user(user_id)
	print "Check FTP: ", check_ftp
	if check_ftp is False:
		print "Bad input - no ftp"
		return -1, "error: Bad Input: User does not have ftp location"
	connection_string = "ftp://%s:%s@ftp.wiser.com/usersftp/LocalUser/User%s/Download/wp_inv_%s.csv" % (ftp_user, ftp_pass, user_id, store_id)
	
	# checking scheduled_at time
	print "getting scheduled_at..."
	gmt_timezone, schedule_at = get_schedule(gmt_timezone,schedule_at)
	if pd.isnull(include_timestamp):
		include_timestamp = 0

	# get store type
	print "getting store type..."
	store_type, is_amazon, is_ebay, ftp_mapping = get_store_type(store_id)

	# check if msrp is set in store
	print "checking store fields..."
	product_fields = get_product_fields(store_id, is_ebay)

	# get custom column names
	print "adding custom columns..."
	custom_attr_fields = get_custom_columns(store_id)

	# get and parse prefs
	print "parsing preferences..."
	preferences = get_prefs(store_id)
  # map_screenshots, price_includes_shipping, is_uom, csv_delimiter, competitor_per_row, max_comps, competitor_store_data_fields, competitor_fields, only_new_price, only_valid_wiseprice

	settings_json = json.dumps({
        "is_amazon": str(is_amazon),
        "is_ebay": str(is_ebay),
        "competitor_per_row_format": str(preferences['competitor_per_row']),
        "report_strategy": "CompetitorReport",
        "max_competitor_count": preferences['max_comps'],
        "fields": {
            "product_fields": product_fields,
            "custom_attr_fields": custom_attr_fields,
            "competitor_store_data_fields": preferences['competitor_store_data_fields'],
            "competitor_fields": preferences['competitor_fields']
        }
    })

	report_data = (
	    str(store_id),
	    "0", #compress
	    str(int(include_timestamp)), #timestamp in filename
	    connection_string,
	    "1440", # 1440 = 24 hours recurrence
	    str(gmt_timezone), # -8 = pst
	    schedule_at.strftime("%Y-%m-%d %H:%M:%S"), ## schedule_at #"2015-12-07 08:00:00",   #(datetime.strptime(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),("%Y-%m-%d %H:%M:%S"))-timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S"), # timezone is utc  
	    "pending", #status
	    preferences['csv_delimiter'], #delimiter
	    settings_json,
	    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
	    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
	)

	### VALIDATIONS ###
	if schedule_at == -1:
		return -1, "error: incorrect schedule time - %s" % gmt_timezone
	if include_timestamp != 0 and include_timestamp != 1:
		return -1, "error: include_timestamp must be 1 or 0"
	if int(preferences['map_screenshots']) > 0:
		return -1, "error: No map screenshots yet."
	if int(preferences['price_includes_shipping']) > 0:
		return -1, "error: No price_includes_shipping yet."
	if int(preferences['is_uom']) > 0:
		return -1, "error: No uom stores just yet."
	if preferences['only_valid_wiseprice'] == 1 or preferences['only_new_price'] == 1:
		return -1, "error: These settings are not available - only_new_price, only_valid_wiseprice"

	return 0, report_data

