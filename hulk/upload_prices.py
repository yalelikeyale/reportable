import os
import time
import pandas as pd
import pandas.io.sql as psql
import pysftp
import paramiko
import select
import sys

from reports import config
from reports import simple_ftp as sf
from reports import report_settings as rs

ftp_creds = config.ftp_creds
hulk_creds = config.hulk_creds

h_host = hulk_creds['host']
h_port = hulk_creds['port']
h_user = hulk_creds['username']
h_pass = hulk_creds['password']
h_path = hulk_creds['path']
keypath =hulk_creds['key']


def run_hulk_command(store_id, comp_id):
  my_agentkey = paramiko.RSAKey.from_private_key_file(keypath, password=h_pass)
  client = paramiko.SSHClient()
  client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
  print "connecting..."
  client.connect( hostname = h_host, username = h_user, pkey = my_agentkey )
  print "connected"
  cmd = 'php /var/WisePricer/wisepricer/wpyii/protected/yiic.php uploadprices doFTP --store_id={0} --comp_store_id={1}'.format(str(store_id), str(comp_id))
  stdin , stdout, stderr = client.exec_command(cmd)
  # Wait for the command to terminate
  while not stdout.channel.exit_status_ready():
      # Only print data if there is data to read in the channel
      if stdout.channel.recv_ready():
          rl, wl, xl = select.select([stdout.channel], [], [], 0.0)
          if len(rl) > 0:
              # Print data from stdout
              print stdout.channel.recv(1024),
  print "done"
  print stdout.channel.recv(1024)
  client.close()

def upload_prices(filename, store_id, comp_id):
  print filename
  ogfilename = filename.replace(" ", "\\ ")
  print ogfilename
  filename = "comp_{0}_{1}.csv".format(store_id, comp_id)
  print filename
  os.renames(ogfilename, filename)
  user_id = rs.get_user(store_id)
  drop_path = ftp_creds['path'] % user_id + 'Upload'

  sf.ftp_drop(
    ftp_creds['host'],
    ftp_creds['port'],
    ftp_creds['username'],
    ftp_creds['password'],
    drop_path,
    filename
  )

  run_hulk_command(store_id, comp_id)

