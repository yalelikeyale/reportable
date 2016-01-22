# -*- coding: UTF-8 -*- 
# URL Aggregation for Michaels
# Created By: Adam David
# Created On: 21-10-2015

"""Downloads Google Shopping competitor URLs from FTP, writes to csv(sku,url), transfers to hulk, then uploads urls to dynamo"""

from ftplib import FTP
import subprocess as sp
import pandas as pd
import MySQLdb
import paramiko
import pysftp
import select
import time
import sys
import os

h_host = '74.86.96.234' #'hulk01.wiser.com'
h_port = 22
h_user = os.environ['HULK_USER']
h_pass = os.environ['HULK_PASS']
h_path = "/home/adam/appUploads/"
keypath = os.environ['HULK_KEY']
ftphost = 'ftp.wiser.com'
ftpport = '2221'
ftpuser = 'User3527'
ftppass = ''
ftppath = '/Download/'
store = '1355760'
scrapers = ['store.scrapbook.com','joann.com','partycity.com','partycity.ca','shop.hobbylobby.com','deserres.com','dickblick.com','jerrysartarama.com','target.com_michaels','walmart.com_michaels']
sitenames = ['scrapbook.com','joann','partycity','partycity','hobbylobby','deserres','dickblick','jerrysartarama','target','walmart']

def ftp_grab(store):
	readfile = "wp_inv_%s.csv" % store
	ftp = FTP()
	ftp.connect(ftphost, ftpport)
	ftp.login(ftpuser, ftppass)
	print "logged in"
	ftp.cwd(ftppath)
	print "grabbing...", readfile, time.strftime("%c")
	with open(readfile, 'wb') as f:
		ftp.retrbinary('RETR %s' % readfile, f.write)
	return readfile

def ftp_drop(filelist):
	my_agentkey = paramiko.RSAKey.from_private_key_file(keypath, password=h_pass)
	agent = paramiko.Agent()
	agent_keys = agent.get_keys() + (my_agentkey,)
	print "key agent"
	for filename in filelist:
		remotepath = h_path+filename
		with pysftp.Connection(h_host, username=h_user, private_key=keypath, private_key_pass=h_pass) as sftp: #"~/.ssh/wiser"
			print "connected"
			sftp.put("./"+filename,remotepath)
		print "ftp drop: ", filename, time.strftime("%c")

def hulk_upload_urls(store_id, filename, scrapername):
	filepath = "/home/adam/appUploads/%s" % filename
	my_agentkey = paramiko.RSAKey.from_private_key_file(keypath, password=h_pass)
	client = paramiko.SSHClient()
	client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	print "connecting..."
	client.connect( hostname = h_host, username = h_user, pkey = my_agentkey )
	print "connected"
	cmd = "php /var/www/front/app/wpyii/protected/yiic userCustom UploadCSVToPinpoint --store_id=%s --path=%s --scraper=%s --skip_first_line=1" % (str(store_id), str(filepath), str(scrapername))
	print "running: \n", cmd
	stdin , stdout, stderr = client.exec_command(cmd)
	# Wait for the command to terminate
	if stdin:
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



### MAIN ###

readfile = ftp_grab(store)
#readfile = "wp_inv_%s.csv" % store
df = pd.read_csv(readfile, dtype=str)
df.fillna("not applicable", inplace=True)
filelist = []
for scraper in scrapers:
	filename = 'urlsfor_%s_%s.csv' % (scraper.replace('.',''), store)
	subdf = df[df['Competitor URL'].str.contains(sitenames[scrapers.index(scraper)])]
	urldf = pd.DataFrame({}, columns=["sku", "url"])
	urldf['sku'] = subdf["Inventory Number"]
	urldf['url'] = subdf["Competitor URL"]
	print urldf
	urldf.to_csv(filename, index=False)
	filelist.append(filename)
print "uploading files:", filelist
ftp_drop(filelist)
for filename in filelist:
	scrapername = scrapers[filelist.index(filename)].replace('.','')
	hulk_upload_urls(store, filename, scrapername)





