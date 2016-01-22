import os

aurora = {
    "username": "",
    "password": "",
    "host": "",
    "port": 3306,
    "database": ""
}

mysqlcon = {
	"host": '',
	"port": 3306,
	"username": os.environ['MYSQL_USER'],
	"password": os.environ['MYSQL_PASS'],
	"database": 'wp_data_prod'
}

wiser_redshift_etl = {
   'user': '',
   'password': '',
   'host': '',
   'database': 'dwh',
   'raise_on_warnings': True,
   'port' : 5439
}

ftp_creds = {
	'user': os.environ['FTP_USER'],
	'password': os.environ['FTP_PASS'],
  'host': '',
  'port': '21',
  'path': '/usersftp/LocalUser/%s/'
}

shorturl_api_key = ''