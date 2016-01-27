# -*- coding: UTF-8 -*- 
import os
import time
import MySQLdb
import pandas as pd
import pandas.io.sql as psql
import subprocess as sp
from ftplib import FTP
from email import Encoders
from reports import fun_url
from reports import config
mysqlcon = config.mysqlcon


stores = [['1162553715', 'USA'], ['1163016056', 'UK'], ['1170967056','Italy'], ['1170967153','Germany']]

OUT_FILEPATH = "mapReport_%s_%s.xlsx"
DROP_PATH = "/Download/"
MYSQL_HOST = mysqlcon['host']
MYSQL_PORT = mysqlcon['port']
MYSQL_USER = mysqlcon['username']
MYSQL_PASS = mysqlcon['password']
MYSQL_DB = 'wp_data_prod'


def create_report(store, filename):
  print filename
  con = MySQLdb.connect(host = MYSQL_HOST, port = MYSQL_PORT, user = MYSQL_USER, 
                         passwd=MYSQL_PASS, db=MYSQL_DB)

  qry =    """(SELECT pps.name AS "Product Name", prod.upc AS "UPC/EAN", pps.store_price AS "Product Price", (SELECT pca.att_value FROM pps_custom_attributes AS pca WHERE pca.pps_id = pps.id AND pca.att_name = "str_color") AS "Color", (SELECT pca.att_value FROM pps_custom_attributes AS pca WHERE pca.pps_id = pps.id AND pca.att_name = "str_material") AS "Material", (SELECT pca.att_value FROM pps_custom_attributes AS pca WHERE pca.pps_id = pps.id AND pca.att_name = "str_size") AS "Size", stores.store_name AS "Retailer", pricing.price AS "Retailer Price", pricing.url AS "Retailer URL", mvs.image_url AS "Violator Screenshot"
          FROM products_per_store AS pps
          JOIN products AS prod ON prod.id = pps.product_id
          JOIN pps_custom_attributes AS pca ON pca.pps_id = pps.id
          JOIN compete_settings AS cs ON cs.store_id = pps.store_id
          JOIN stores ON cs.compete_id = stores.id
          JOIN pricing ON pricing.store_id = cs.compete_id AND pricing.product_id = pps.product_id
          LEFT JOIN map_violators_screenshots AS mvs ON (mvs.pps_id = pps.id AND mvs.date >= now() - INTERVAL 1 DAY AND mvs.site_name = prod.asin AND pricing.url LIKE "%amazon%")
          OR (mvs.pps_id = pps.id AND mvs.date >= now() - INTERVAL 1 DAY AND pricing.url NOT LIKE "%amazon%" AND mvs.site_name = prod.upc)
          WHERE cs.enabled = 1 AND cs.verified = 1
          AND pricing.price < pps.store_price AND pps.store_id = {0}  
          GROUP BY prod.upc, stores.id)
          UNION
          (SELECT pps.name, prod.upc, pps.store_price AS "Product Price", (SELECT pca.att_value FROM pps_custom_attributes AS pca WHERE pca.pps_id = pps.id AND pca.att_name = "str_color") AS "Color", (SELECT pca.att_value FROM pps_custom_attributes AS pca WHERE pca.pps_id = pps.id AND pca.att_name = "str_material") AS "Material", (SELECT pca.att_value FROM pps_custom_attributes AS pca WHERE pca.pps_id = pps.id AND pca.att_name = "str_size") AS "Size", stores.store_name, pricing_ne.price AS "Retailer Price", pricing_ne.url AS "Retailer URL", mvs.image_url AS "Violator Screenshot"
          FROM products_per_store AS pps
          JOIN products AS prod ON prod.id = pps.product_id
          JOIN pps_custom_attributes AS pca ON pca.pps_id = pps.id
          JOIN compete_settings AS cs ON cs.store_id = pps.store_id
          JOIN stores ON cs.compete_id = stores.id
          JOIN pricing_ne ON pricing_ne.store_id = cs.compete_id AND pricing_ne.pps_id = pps.id
          JOIN guli_pricing AS gs ON gs.pricing_id = pricing_ne.id
          LEFT JOIN map_violators_screenshots AS mvs ON (mvs.pps_id = pps.id AND mvs.site_name NOT LIKE prod.asin AND mvs.date  >= now() - INTERVAL 1 DAY)
          WHERE cs.enabled = 1 AND gs.action LIKE "insert" AND pricing_ne.price < pps.store_price AND pps.store_id = {1}
          GROUP BY prod.upc, stores.id)
          ORDER BY "Product Name", "Material", "Color", "Retailer";"""

  store = str(store)
  print store
  qry = qry.format(store,store)
  resultdata = pd.read_sql(qry, con = con)

  # Shorten URLs
  f = lambda x: fun_url.clean_url(x)
  resultdata['Retailer URL'] = resultdata['Retailer URL'].apply(f)

  # Sort and pivot data
  resultdata.sort(['Product Name', 'Material', 'Color', 'Retailer'], inplace=True)
  writer = pd.ExcelWriter(filename, engine='xlsxwriter')
  resellers = resultdata.pivot_table(index=["Retailer", "Product Name", "Retailer URL"])
  sellertotals = resultdata[["Retailer", "Product Name"]].groupby(["Retailer"]).agg(["count"])
  sellertotals.columns = sellertotals.columns.get_level_values(0)
  sellertotals = sellertotals.reset_index()
  resellers = resellers.reset_index()
  resellers = pd.merge(resellers, sellertotals, left_on="Retailer", right_on="Retailer")
  resellers.rename(columns={'Product Name_x': 'Product Name', 'Product Name_y': 'Violation Count'}, inplace=True)
  resellers.to_excel(writer, sheet_name="Resellers", index=False)
  resultdata.to_excel(writer, sheet_name='DataSet', index=False)
  pt = pd.pivot_table(resultdata, index=["Product Name", "Material", "Color", "Retailer", "Retailer Price"])
  pt.to_excel(writer, sheet_name='PivotTable')
  writer.save()
  print "export " + time.strftime('%F')

def send_email(filenames, recip, bcc):
  import smtplib
  import mimetypes
  from email.mime.multipart import MIMEMultipart
  from email import encoders
  from email.message import Message
  from email.mime.base import MIMEBase
  from email.mime.text import MIMEText
  
  msg = MIMEMultipart()

  attachments = filenames

  for filename in attachments:
    f = filename
    part = MIMEBase('application', "octet-stream")
    part.set_payload( open(f,"rb").read() )
    Encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f))
    msg.attach(part)

  emailrecip = recip
  emailto = ", ".join(emailrecip)
  
  emailfrom = os.environ['UL_USER']
  username = os.environ['UL_USER']
  password = os.environ['UL_PASS']
  msg["From"] = emailfrom
  msg["To"] = emailto
  msg["Subject"] = "WiseReport - MAP Policy Updated Violators"
  msg.preamble = "WiseReport - MAP Policy Updated Violators"

  print "attaching message..."
  server = smtplib.SMTP("smtp.gmail.com:587")
  server.starttls()
  server.login(username,password)
  emailrecip = emailrecip + bcc
  server.sendmail(emailfrom, emailrecip, msg.as_string())
  print "sent message to %s" % emailrecip
  server.quit()


## MAIN ##
bcc = ["adam.david@wiser.com", "dean@wiser.com"]
filenamesUs = ["JLelonek@stuartweitzman.com","AStarr@stuartweitzman.com","wkulkin@stuartweitzman.com"]
filenamesEu = ["AZalka@stuartweitzman.com","AStarr@stuartweitzman.com","wkulkin@stuartweitzman.com"]
for store in stores:
  filename = OUT_FILEPATH % (store[1], time.strftime('%F'))
  if store[1] == "USA":
    filenamesUs.append(filename)
  else:
    filenamesEu.append(filename)
  create_report(int(store[0]), filename)
send_email(filenamesUs, usrecip, bcc)
send_email(filenamesEu, eurecip, bcc)
sp.call("rm mapReport*", shell=True)
