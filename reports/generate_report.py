# -*- coding: UTF-8 -*- 
#pckg
import csv
import time
import psycopg2
import numpy as np
import pandas as pd
import pandas.io.sql as psql
import subprocess as sp
from ftplib import FTP
from pandas import concat
#mod
import report_settings as rs
import config

ftp_creds = config.ftp_creds
wiser_redshift_etl = config.wiser_redshift

# stores = ['579991', '1162612429']
# user_id = '2595'
# username = 'User%s' % user_id
# ftppath = ftp_creds['path'] % username
# fileName = "wp_inv_%s.csv"

db = psycopg2.connect(host=wiser_redshift_etl['host'], user=wiser_redshift_etl['username'], password=wiser_redshift_etl['password'], database=wiser_redshift_etl['database'], port=wiser_redshift_etl['port'], connect_timeout=5)

def get_comp_settings(store_id, user_id):
	'''Return the store settings related to competitor data'''
	prefs = rs.get_prefs(store_id, user_id)
	try:
		max_comps = max_comps
	except Exception, e:
		print e
		max_comps = prefs['max_comps']
	try:
		comp_url = comp_url
	except Exception, e:
		print e
		comp_url = prefs['comp_url']
	try:
		price_w_ship = price_w_ship
	except Exception, e:
		print e
		price_w_ship = prefs['price_includes_shipping']
	return {'max_comps': int(max_comps), 'comp_url': int(comp_url), 'price_w_ship': int(price_w_ship)}

def get_product_data(store_id):
	'''Get product data based on current store settings or manual overrides'''
	product_fields = product_fields or rs.get_product_fields(store_id)

	query = """SELECT prod.name AS "Product Name", prod.sku AS "Inventory Number",
				prod.stock_level AS "IN STOCK", prod.upc AS "UPC/EAN", prod.asin, 
				prod.brand AS "Make", prod.model AS "Model", prod.mpn,
				prod.store_price AS "Product Price", prod.min_price AS "Minimum Price",
				prod.max_price AS "Maximum Price", prod.store_cost AS "Cost",
				prod.store_ship AS "Shipping Price", prod.product_url AS "Product URL",
				prod.image_url AS "Image URL", ROUND(prod.wiseprice, 2) AS "New Price",
				(SELECT pl.keyword FROM product_labels AS pl WHERE prod.ppsid = pl.ppsid) AS "Labels" 
				FROM products as prod
				WHERE prod.store_id = {0}"""

	product_data = pd.read_sql(query.format(store_id), db)
	return product_data

def get_custom_column_data(store_id):
	'''Grab all custom columns and values based on columns in store'''
	custom_columns = rs.get_custom_columns(store_id)
	data_list = []
	for column in custom_columns:
		query = """SELECT prod.sku, (select att_value from pps_custom_attributes as pca where pca.ppsid = prod.ppsid and pca.att_name = {0})
					FROM products as prod
					where prod.store_id = "{1}" """
		data_list.append(pd.read_sql(query.format(column, store_id), db).set_index('sku'))
	custom_column_data = pd.concat(data_list, axis=1)
	return custom_column_data


def get_competitor_data(store_id):
	'''Get competitor data in comp per row format based on
	current store settings or manual overrides'''

	query = """SELECT prod.sku, p.store_name AS "Comp", p.price AS "Comp Price", p.ship AS "Comp Shipping", p.url AS "Comp URL", prod.competitors_count AS "Total Competitors"
				FROM products AS prod
				JOIN stores as client_store on client_store.id = prod.store_id
				LEFT JOIN pricing AS p ON p.ppsid = prod.ppsid AND p.approved = 1 AND GETDATE()-p.last_update <= INTERVAL '7 days' AND p.store_name <> client_store.store_name
				LEFT JOIN compete_settings AS cs ON prod.store_id = cs.store_id AND p.store_id = cs.compete_id AND cs.id IS NULL
				LEFT JOIN stores as comp_store on comp_store.id = p.store_id AND client_store.store_url <> comp_store.store_url
				WHERE prod.store_id = %s"""

	competitor_data = pd.read_sql(query % store_id, db)
	return competitor_data

def top_competitor_format(store_id, user_id):
	'''Transform competior data to rows unique by sku instead of (sku, competitor).
	Competitor formats: (Comp1, Comp1 Price Includes Shiping, Comp1 URL, Comp2...)
					    (Comp1, Comp1 Price, Comp1 Shiping, Comp1 URL, Comp2...)
					    [URL optional]'''

	comp_prefs = get_comp_settings(store_id, user_id)
	max_comps = comp_prefs['max_comps']
	comp_url = comp_prefs['comp_url']
	price_w_ship = comp_prefs['price_w_ship']
	cols_per_comp = 3+comp_url-price_w_ship # 3 (base number of columns ['name', 'price', 'ship']) + comp_url - pricewithshippin (if one their will not be a shipping column) 
	print "Number of columns per competitor:", cols_per_comp

	print time.strftime("%c"), "getting comp data..."
	competitor_data = get_competitor_data(store_id)
	# Uncomment below to test from file (be sure to comment out the query above to save load)
	# competitor_data.to_csv("temptest.csv", index=False)
	# competitor_data = pd.read_csv("temptest.csv")
	print time.strftime("%c"), "shape of comp data:", competitor_data.shape

	# titleize column headers
	competitor_data.columns = map(lambda x: x.title(), competitor_data.columns.tolist())
	# create price includes shipping column
	competitor_data.insert(3,'Comp Price Includes Shipping', competitor_data['Comp Price'] + competitor_data['Comp Shipping'])
	# sort df by sku and comp offer
	sortedresult = competitor_data.sort(['Sku', 'Comp Price Includes Shipping'], ascending=[0,1])
	print time.strftime("%c"), "sorted"
	competitor_data = None

	groupedresult = sortedresult.groupby(['Sku']).head(int(max_comps)).groupby(['Sku']) # top max_comps cheapest competitors (by lowest price)
	print time.strftime("%c"), "grouped"
	sortedresult = None

	# transpose comps to columns
	transposed = pd.concat((groupedresult['Sku'].apply(lambda x: pd.Series(data=str(x.values))).unstack(),
							groupedresult['Comp'].apply(lambda x: pd.Series(data=x.values)).unstack(),
							groupedresult['Comp Price Includes Shipping'].apply(lambda x: pd.Series(data=x.values)).unstack(),
							groupedresult['Comp Price'].apply(lambda x: pd.Series(data=x.values)).unstack(),
							groupedresult['Comp Shipping'].apply(lambda x: pd.Series(data=x.values)).unstack(),
							groupedresult['Comp Url'].apply(lambda x: pd.Series(data=x.values)).unstack()),
	                keys= ['Sku', 'Comp', 'Comp Price Includes Shipping', 'Comp Price', 'Comp Shipping', 'Comp URL'], axis=1)
	
	# Drop unnecessary columns
	groupedresult = None
	if price_w_ship:
		transposed.drop(['Comp Price', 'Comp Shipping'], axis=1, inplace=True)
	else:
		transposed.drop(['Comp Price Includes Shipping'], axis=1, inplace=True)
	if not comp_url:
		transposed.drop(['Comp URL'], axis=1, inplace=True)

	transposed.columns = map(lambda x: "Sku" if x[0]=="Sku" else x[0][0:4]+str(x[1]+1)+x[0][4:], transposed.columns.tolist()) # colapse multi index to numbered columns
	print time.strftime("%c"), "transposed"
	transposed = transposed.reset_index(drop=True)

	# fix skus
	f = lambda x: str(x.split(' ')[0][1:].replace("'", "").replace(']','').replace(',','')) # sku list to single sku
	transposed['Sku'] = transposed['Sku'].map(f)

	numcomps = (len(transposed.columns.tolist())-1)/cols_per_comp
	neworder = [transposed.columns[0]]

	# interleave competitors according to Comp#
	for i in range(1,numcomps+1):
	 	if price_w_ship:
	 		if comp_url:
	 			neworder.extend([transposed.columns[i], transposed.columns[i+numcomps], transposed.columns[i+numcomps*2]])
	 		else:
	 			neworder.extend([transposed.columns[i], transposed.columns[i+numcomps]])
	 	else:
	 		if comp_url:
	 			neworder.extend([transposed.columns[i], transposed.columns[i+numcomps], transposed.columns[i+numcomps*2], transposed.columns[i+numcomps*3]])
	 		else:
	 			neworder.extend([transposed.columns[i], transposed.columns[i+numcomps], transposed.columns[i+numcomps*2]])
	finalresult = transposed[neworder]

	# Add emtpy competitor columns to keep constant column count
	for i in range(numcomps, max_comps+1):
		if price_w_ship:
			finalresult["Comp%s" % i] = ""
			finalresult["Comp%s Price Includes Shipping" % i] = ""
		else:
			finalresult["Comp%s" % i] = ""
			finalresult["Comp%s Price" % i] = ""
			finalresult["Comp%s Shipping" % i] = ""

	return finalresult


def top_competitor_report(store_id, user_id):

	prods = pd.concat([get_product_data(store_id).set_index("Inventory Number"),
    	               get_custom_column_data(store_id).set_index("sku")], axis=1)

	finalresult = pd.merge(prods, transposed, how='outer', on='Inventory Number')
	cols = list(finalresult.columns)
	totcomps = finalresult['Total Competitors']
	finalresult.drop(labels=['Total Competitors'], axis=1,inplace = True)
	finalresult['Total Competitors'] = totcomps
	print time.strftime("%c"), "merged"
	print "shape of merge:", finalresult.shape


