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
    "username": os.environ['AURORA_USER'],
    "password": os.environ['AURORA_PASS'],
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
    'username': os.environ['FTP_USER'],
    'password': os.environ['FTP_PASS'],
  'host': 'ftp.wiser.com',
  'port': '21',
  'path': '/usersftp/LocalUser/%s/'
}

hulk_creds = {
  'host': os.environ['HULK_HOST'],
  'port': 22,
  'username': os.environ['HULK_USER'],
  'password': os.environ['HULK_PASS'],
  'key': os.environ['HULK_KEY'],
  'path': os.environ['HULK_PATH']
}

shorturl_api_key = ''