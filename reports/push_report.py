import pandas as pd
from config import aurora
import reportdata

import pymysql as mdb
import sys

con = mdb.connect(host=aurora["host"], user=aurora["username"], passwd=aurora["password"], db=aurora["database"])
cur = con.cursor()

# mcon = mdb.connect(host='127.0.0.1', user='root', db='wisereports_dev')
# mcur = con.cursor()


report_data = reportdata.report_data

def has_duplicate(report_data):
  sid = report_data[0]
  tstamp = report_data[2]
  constring = report_data[3]
  recurrence = report_data[4]
  gmt_timezone = report_data[5]
  schedule_at = report_data[6]

  qry = """SELECT count(*) as result
    FROM reports
    WHERE store_id = {0}
    AND include_timestamp_in_filename = {1}
    AND target_connection_string = "{2}"
    AND recurrence_in_minutes = {3}
    AND gmt_timezone = {4}
    AND (scheduled_at = "{5}" OR scheduled_at = DATE_ADD("{5}", INTERVAL 1 DAY));
    """

  try:
    result = pd.read_sql(qry.format(sid, tstamp, constring, recurrence, gmt_timezone, schedule_at), con=con)
    result = result.loc[0]["result"]
  except Exception, e:
    print e
    return -1
  else:
    if result > 0:
      return True
    else:
      return False


def create_row(report_data):
  # if has_duplicate:
  #   return "Duplicate Record!  Row not created."

  try:
    print """INSERT INTO reports (store_id, compress, include_timestamp_in_filename,target_connection_string, recurrence_in_minutes, gmt_timezone,scheduled_at,status,csv_delimiter,report_data,created_at,updated_at) VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')
    """ % report_data
    cur.execute("""INSERT INTO reports (
    store_id, 
    compress, 
    include_timestamp_in_filename,
    target_connection_string, 
    recurrence_in_minutes,
    gmt_timezone,
    scheduled_at,
    status,
    csv_delimiter,
    report_data,
    created_at,
    updated_at
      ) VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % report_data)
    con.commit()
  except mdb.Error, e:
    print "Error %d: %s -- Rolling Back!" % (e.args[0], e.args[1])
    con.rollback()
    sys.exit(1)
  finally:
    if con:
        con.close()
  
create_row(report_data)
