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

db = psycopg2.connect(host=wiser_redshift_etl['host'], user=wiser_redshift_etl['username'], password=wiser_redshift_etl['password'], database=wiser_redshift_etl['database'], port=wiser_redshift_etl['port'], connect_timeout=5)

def get_comp_settings(store_id):
	'''Return the store settings related to competitor data'''
	prefs = rs.get_prefs(store_id)
	try:
		max_comps = max_comps
	except Exception, e:
		max_comps = prefs['max_comps']
	try:
		comp_url = comp_url
	except Exception, e:
		comp_url = prefs['comp_url']
	try:
		price_w_ship = price_w_ship
	except Exception, e:
		price_w_ship = prefs['price_includes_shipping']
	return {'max_comps': int(max_comps), 'comp_url': int(comp_url), 'price_w_ship': int(price_w_ship)}

def get_product_data(store_id, filters={}):
	'''Get product data based on current store settings or manual overrides'''
	product_fields = rs.get_product_fields(store_id)

	query = """SELECT prod.ppsid, prod.product_id, prod.name AS "Product Name",
					prod.sku AS "Inventory Number", prod.stock_level AS "IN STOCK",
					prod.upc AS "UPC/EAN", prod.asin, prod.epid, prod.brand AS "Make", prod.model AS "Model",
					prod.mpn, prod.store_price AS "Product Price", prod.min_price AS "Minimum Price",
					prod.max_price AS "Maximum Price", prod.store_cost AS "Cost",
					prod.store_ship AS "Shipping Price", prod.product_url AS "Product URL",
					prod.image_url AS "Image URL", ROUND(prod.wiseprice, 2) AS "New Price",
					(SELECT listagg(pl.keyword, ', ') FROM product_labels AS pl WHERE prod.ppsid = pl.ppsid) AS "Labels",
					prod.msrp, prod.competitors_count as "total competitors" 
					FROM products as prod
					WHERE prod.deleted = 0 and prod.store_id = {0}"""
	query = query.format(store_id)

	if 'brands' in filters:
		brands = '|'.join(filters['brands'])
		print 'brands: ', brands
		query = query + " AND LOWER(prod.brand) SIMILAR TO LOWER('%({0})%')".format(brands)

	product_data = pd.read_sql(query, db)

	drop_fields = []
	if "msrp" not in product_fields:
		drop_fields.append("msrp")
	if "epid" not in product_fields:
		drop_fields.append("epid")
	product_data.drop(drop_fields, axis=1, inplace=True)
	return product_data

def get_custom_column_data(store_id):
	'''Grab all custom columns and values based on columns in store'''
	custom_columns = rs.get_custom_columns(store_id)
	data_list = []
	for column in custom_columns:
		query = """SELECT prod.sku, (select att_value from pps_custom_attributes as pca where pca.ppsid = prod.ppsid and pca.att_name = '{0}') as {0}
								FROM products as prod
								where prod.store_id = {1}"""
		data_list.append(pd.read_sql(query.format(column, store_id), db).set_index('sku'))
	try:
		custom_column_data = pd.concat(data_list, axis=1)
	except Exception, e:
		print 'no custom columns'
		return pd.DataFrame()
	custom_column_data['inventory number'] = custom_column_data.index
	custom_column_data.reset_index(drop=True)
	return custom_column_data

def get_select_statement(columns):
	table_abr = { 
		'products': 'prod',
		'stores': 'client_store',
		'client_store': 'client_store',
		'comp_store': 'comp_store',
		'compete_settings': 'cs',
		'pricing': 'p',
		'map_violators_screenshots': 'mvs',
		'product_labels': 'pl',

	}
	print "Columns: %s" % columns
	select_statement = ''
	for table in columns:
		print "Table: %s" % table
		for column in columns[table]:
			print "Current Column: %s" % column
			if table == 'pps_custom_attributes':
				heading = """, (select att_value from pps_custom_attributes as pca where pca.ppsid = prod.ppsid and pca.att_name = '{0}') as {0}""".format(column)
			elif table == 'product_labels' and column == 'keyword':
				heading = """, (SELECT listagg(pl.keyword, ', ') 
										FROM product_labels AS pl 
										WHERE prod.ppsid = pl.ppsid) AS labels"""
			else:
				heading = ", {0}.{1}".format(table_abr[table],column)
			select_statement = select_statement + heading
	return select_statement

def get_screenshots_statement(columns, screenshots):
	screenshots_statement = ''
	if screenshots or 'map_violators_screenshots' in columns:
		screenshots_statement = """LEFT JOIN map_violators_screenshots AS mvs ON
				(mvs.ppsid = prod.ppsid AND mvs.date >= GETDATE() - INTERVAL '4 days') AND
        ((mvs.site_name ilike prod.asin AND p.url LIKE '%amazon.com%') OR
        (p.url NOT LIKE '%amazon.com%' AND mvs.site_name like '%'+prod.upc))"""
	return screenshots_statement

def query_competitor_data(store_id, columns={'products': ['sku']}, filters={}, screenshots=False, dedup=False):
	query = """SELECT prod.sku, cs.id as csid {0}
							FROM products AS prod
							JOIN stores as client_store on client_store.id = prod.store_id
							LEFT JOIN pricing AS p ON p.ppsid = prod.ppsid AND p.approved = 1 AND p.store_name <> client_store.store_name and prod.product_id = p.product_id
							LEFT JOIN compete_settings AS cs ON prod.store_id = cs.store_id AND p.store_id = cs.compete_id
							LEFT JOIN stores as comp_store on comp_store.id = p.store_id AND client_store.store_url <> comp_store.store_url
							{1}
							WHERE prod.deleted = 0 AND prod.store_id = {2}"""
	query = query.format(get_select_statement(columns), get_screenshots_statement(columns, screenshots), store_id)
	print "Running User Query: %s" % query
	data = pd.read_sql(query, db)
	data = data[pd.isnull(data['csid'])]
	return data

# filters={'brands': ["dwalt"], 'competitors': ["staples.com"]}
def get_competitor_data(store_id, filters={}, dedup=False):
	'''Get competitor data in comp per row format based on
	current store settings or manual overrides'''

	# no longer filtering by date (AND GETDATE()-p.last_update <= INTERVAL '7 days')
	query = """SELECT prod.sku, cs.id "csid", p.store_name AS "Comp", p.price AS "Comp Price", p.ship AS "Comp Shipping", p.url AS "Comp URL", mvs.image_url AS "Comp Violator Screenshot", prod.competitors_count AS "Total Competitors"
				FROM products AS prod
				JOIN stores as client_store on client_store.id = prod.store_id
				LEFT JOIN pricing AS p ON p.ppsid = prod.ppsid AND p.approved = 1 AND p.store_name <> client_store.store_name and prod.product_id = p.product_id
				LEFT JOIN compete_settings AS cs ON prod.store_id = cs.store_id AND p.store_id = cs.compete_id
				LEFT JOIN stores as comp_store on comp_store.id = p.store_id AND client_store.store_url <> comp_store.store_url
				LEFT JOIN map_violators_screenshots AS mvs ON
        (mvs.ppsid = prod.ppsid AND mvs.date >= GETDATE() - INTERVAL '4 days') AND
        ((mvs.site_name ilike prod.asin AND p.url LIKE '%amazon.com%')
              OR (p.url NOT LIKE '%amazon.com%' AND mvs.site_name like '%'+prod.upc))
				WHERE prod.deleted = 0 AND prod.store_id = {0}"""
	query = query.format(store_id)
	print "filters: ", filters
	if 'brands' in filters:
		brands = '|'.join(filters['brands'])
		print 'brands: ', brands
		query = query + " AND LOWER(prod.brand) SIMILAR TO LOWER('%({0})%')".format(brands)
	if 'competitors' in filters:
		competitors = '|'.join(filters['competitors'])
		print 'comps: ', competitors
		query = query + " AND LOWER(comp_store.store_name) SIMILAR TO LOWER('%({0})%')".format(competitors)
	if 'comp_url' in filters:
		comp_url = '|'.join(filters['comp_url'])
		print 'comp_urls: ', comp_url
		query = query + " AND LOWER(p.url) SIMILAR TO LOWER('%({0})%')".format(comp_url)
	print query
	competitor_data = pd.read_sql(query, db)
	competitor_data = competitor_data[pd.isnull(competitor_data['csid'])]
	if dedup:
		print "deduping...."
		competitor_data = competitor_data.sort(columns=['sku', 'comp price'], ascending=True).groupby(['sku', 'comp']).head(1)
	competitor_data.drop(['csid'], axis=1, inplace=True)
	return competitor_data

def top_competitor_format(store_id, filters={}, screenshots=False, dedup=False):
	'''Transform competior data to rows unique by sku instead of (sku, competitor).
	Competitor formats: (Comp1, Comp1 Price Includes Shiping, Comp1 URL, Comp2...)
					    (Comp1, Comp1 Price, Comp1 Shiping, Comp1 URL, Comp2...)
					    [URL optional]'''

	comp_prefs = get_comp_settings(store_id)
	max_comps = comp_prefs['max_comps']
	comp_url = comp_prefs['comp_url']
	price_w_ship = comp_prefs['price_w_ship']
	cols_per_comp = 3+comp_url-price_w_ship+screenshots # 3 (base number of columns ['name', 'price', 'ship']) + comp_url - pricewithshippin (if one their will not be a shipping column) 
	print "Number of columns per competitor:", cols_per_comp

	print time.strftime("%c"), "getting comp data..."
	competitor_data = get_competitor_data(store_id, filters, dedup)
	# Uncomment below to test from file (be sure to comment out the query above to save load)
	# competitor_data = pd.read_csv("temptest.csv")
	print time.strftime("%c"), "shape of comp data:", competitor_data.shape

	# titleize column headers
	competitor_data.columns = map(lambda x: x.title(), competitor_data.columns.tolist())
	# create price includes shipping column
	competitor_data.insert(3,'Comp Price Includes Shipping', competitor_data['Comp Price'] + competitor_data['Comp Shipping'])
	# sort df by sku and comp offer
	sortedresult = competitor_data.sort(['Sku', 'Comp Price'], ascending=[0,1])
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
							groupedresult['Comp Url'].apply(lambda x: pd.Series(data=x.values)).unstack(),
							groupedresult['Comp Violator Screenshot'].apply(lambda x: pd.Series(data=x.values)).unstack()),
	                keys= ['Sku', 'Comp', 'Comp Price Includes Shipping', 'Comp Price', 'Comp Shipping', 'Comp URL', 'Comp Violator Screenshot'], axis=1)
	
	# Drop unnecessary columns
	groupedresult = None
	if price_w_ship:
		transposed.drop(['Comp Price', 'Comp Shipping'], axis=1, inplace=True)
	else:
		transposed.drop(['Comp Price Includes Shipping'], axis=1, inplace=True)
	if not comp_url:
		transposed.drop(['Comp URL'], axis=1, inplace=True)
	if not screenshots:
		transposed.drop(['Comp Violator Screenshot'], axis=1, inplace=True)

	transposed.columns = map(lambda x: "Sku" if x[0]=="Sku" else x[0][0:4]+str(x[1]+1)+x[0][4:], transposed.columns.tolist()) # colapse multi index to numbered columns
	print time.strftime("%c"), "transposed"
	transposed = transposed.reset_index(drop=True)

	# fix sku format
	f = lambda x: str(list(x.replace('[','').replace(']','').replace('\n','').replace('\t','').split("' '"))[0].replace("'","")) # sku list to single sku
	transposed['Sku'] = transposed['Sku'].map(f)

	numcomps = (len(transposed.columns.tolist())-1)/cols_per_comp
	neworder = [transposed.columns[0]]

	# interleave competitors according to Comp#
	for i in range(1,numcomps+1):
	 	if price_w_ship:
	 		if screenshots:
		 		if comp_url:
		 			neworder.extend([transposed.columns[i], transposed.columns[i+numcomps], transposed.columns[i+numcomps*2], transposed.columns[i+numcomps*3]])
		 		else:
		 			neworder.extend([transposed.columns[i], transposed.columns[i+numcomps], transposed.columns[i+numcomps*2]])
	 		else:
	 			if comp_url:
		 			neworder.extend([transposed.columns[i], transposed.columns[i+numcomps], transposed.columns[i+numcomps*2]])
		 		else:
		 			neworder.extend([transposed.columns[i], transposed.columns[i+numcomps]])
	 	else:
	 		if screenshots:
		 		if comp_url:
		 			neworder.extend([transposed.columns[i], transposed.columns[i+numcomps], transposed.columns[i+numcomps*2], transposed.columns[i+numcomps*3], transposed.columns[i+numcomps*4]])
		 		else:
		 			neworder.extend([transposed.columns[i], transposed.columns[i+numcomps], transposed.columns[i+numcomps*2], transposed.columns[i+numcomps*3]])
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
		if comp_url:
			finalresult["Comp%s URL" % i] = ""
		if screenshots:
			finalresult["Comp%s Violator Screenshot"] = ""

	finalresult['inventory number'] = finalresult['Sku']
	finalresult.drop('Sku', axis=1, inplace=True)
	return finalresult

# Example Call for generating top_competitor_report:
# from reports import generate_report as gr
# gr.top_competitor_report(1446251, filters={'competitors': ['staples.com']})
# gr.top_competitor_report(1178770744).to_csv("topcompTESTing.csv")
# gr.top_competitor_report(1189273733).to_csv("bctestMSRP.csv")
def top_competitor_report(store_id, filters={}, custom_columns=True, screenshots=False, dedup=False):
	product_data = get_product_data(store_id, filters).drop(["ppsid", "product_id"], axis=1)
	if custom_columns:
		custom_cols = get_custom_column_data(store_id)
		if not custom_cols.empty and len(custom_cols) > 1:
			prods = pd.merge(product_data, custom_cols, how='outer', on='inventory number')
		else:
			prods = product_data
	else:
		prods = product_data
	comp_data = top_competitor_format(store_id, filters, screenshots, dedup)

	prod_column_count = len(prods.columns)
	compcols = comp_data.columns.values.tolist()
	finalresult = pd.merge(prods, comp_data, how='outer', on='inventory number')
	cols = finalresult.columns.values.tolist()[0:prod_column_count]
	f = lambda x: x.title()
	newcols = list(map(f, cols))
	finalcols = newcols + compcols[0:-1]
	finalresult.columns = finalcols

	# prods.to_csv("prods.csv")
	# comp_data.to_csv("comp_data.csv")
	# TODO:
	# FIX THIS BULLSHIT ABOVE

	totcomps = finalresult['Total Competitors']
	finalresult.drop(labels=['Total Competitors'], axis=1,inplace=True)
	finalresult['Total Competitors'] = totcomps
	print time.strftime("%c"), "merged"
	print "shape of merge:", finalresult.shape
	return finalresult

def distinct_row_report(store_id, filters={}, custom_columns=True, screenshots=False, dedup=False):
	product_data = get_product_data(store_id, filters).drop(["ppsid", "product_id"], axis=1)
	custom_cols = get_custom_column_data(store_id)
	if not custom_cols.empty and len(custom_cols) > 1:
		prods = pd.merge(product_data, custom_cols, how='outer', on='inventory number')
	else:
		prods = product_data
	comp_data = get_competitor_data(store_id, filters, dedup)
	comp_data['inventory number'] = comp_data['sku']
	finalresult = pd.merge(prods, comp_data, how='outer', on='inventory number')
	finalresult.rename(columns={'total competitors_y': 'total competitors'}, inplace=True)
	finalresult.drop(['sku', 'total competitors_x'], axis=1, inplace=True)
	return finalresult
 