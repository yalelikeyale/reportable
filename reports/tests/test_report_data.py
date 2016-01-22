import os
import reports
import MySQLdb
import pandas as pd
from config import mysqlcon

INPUT_FILE = "sid_uid.csv"

ftp_user = os.environ["FTP_USER"]
ftp_pass = os.environ["FTP_PASS"]

con = MySQLdb.connect(host = mysqlcon['host'] , port = mysqlcon['port'], user = mysqlcon['username'], 
                  passwd=mysqlcon['password'], db=mysqlcon['database'] )

stores = pd.read_csv(INPUT_FILE)
print stores


### MAIN ###
"""Creates report data based prefs from store_id and uid given in csv
   --not yet -- then calls push report to add a row to aurora  -- for now just prints it out for review"""

stores["result"] = ""
stores["report_data"] = ""
for index, row in stores.iterrows():
  print "### NEW ROW ###: ", index, "-  SID: ", row["store_id"], "-  UID: ", row["user_id"]
  result, report_data = reports.report_settings.generate_report_data(row["store_id"], row["user_id"], row['gmt_timezone'], row['schedule_at'], row['include_timestamp'])
  print "final report_data: ", report_data
  if result != 0:
    stores.loc[index, "result"] = report_data
    stores.loc[index, "report_data"] = "None"
  else:
    stores.loc[index, "result"] = "Success!"
    stores.loc[index, "report_data"] = str(report_data)
print "printing results...."
stores.to_csv("results_you_true.csv")
print "Done!"

