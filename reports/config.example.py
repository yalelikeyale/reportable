import os

# wisereports
aurora = {
  "username": "",
  "password": "",
  "host": "aurora01-cluster.cluster-cnvxtb0vybkd.us-east-1.rds.amazonaws.com",
  "port": 3306,
  "database": "wisereports"
}

# legacy db
mysqlcon = {
  "host": 'db03.cnvxtb0vybkd.us-east-1.rds.amazonaws.com', #'db01.wiser.com',
  "port": 3306,
  "username": "",
  "password": "",
  "database": 'wp_data_prod'
}

# redshift dwh
wiser_redshift = {
  'username': '',
  'password': '',
  'host': 'dwh.crt3yyrvkmj5.us-east-1.redshift.amazonaws.com',
  'database': 'dwh',
  'raise_on_warnings': True,
  'port' : 5439
}

ftp_creds = {
  'username': '',
  'password': '',
  'host': 'ftp.wiser.com',
  'port': '21',
  'path': '/usersftp/LocalUser/%s/'
}

hulk_creds = {
  'host': '',
  'port': 22,
  'username': '',
  'password': '',
  'key': '',
  'path': ''
}

shorturl_api_key = ''