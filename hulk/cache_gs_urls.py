# -*- coding: UTF-8 -*- 
# Purpos: Caches google shopping urls for custom scrapers
# Created By: Adam David
# Created On: 21-10-2015

"""Downloads Google Shopping competitor URLs from FTP, writes to csv(sku,url), transfers to hulk, then uploads urls to dynamo"""
import time
import select
import pysftp
import paramiko
import pandas as pd
import subprocess as sp
from ftplib import FTP
from reports import config
from reports import simple_ftp
from reports import report_settings as rs

hulk_creds = config.hulk_creds
wiser_ftp = config.ftp_creds

h_host = hulk_creds['host']
h_port = hulk_creds['port']
h_user = hulk_creds['username']
h_pass = hulk_creds['password']
h_path = hulk_creds['path']
keypath = hulk_creds['key']
ftphost = wiser_ftp['host']
ftpport = wiser_ftp['port']
ftpuser = wiser_ftp['username']
ftppass = wiser_ftp['password']
store = '1355760'
# scrapers = ['store.scrapbook.com','joann.com','partycity.com','partycity.ca','shop.hobbylobby.com','deserres.com','dickblick.com','jerrysartarama.com','target.com_michaels','walmart.com_michaels']
# sitenames = ['scrapbook.com','joann','partycity','partycity','hobbylobby','deserres','dickblick','jerrysartarama','target','walmart']


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
	cmd = "php /var/WisePricer/wisepricer/wpyii/protected/yiic userCustom UploadCSVToPinpoint --store_id=%s --path=%s --scraper=%s --skip_first_line=1" % (str(store_id), str(filepath), str(scrapername))
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

def cache_urls(store_id, scrapers, sitenames):
	user_id = rs.get_user(store_id)
	ftppath = (wiser_ftp['path'] % ("User%s"%user_id)) + "Download/"
	filename = "wp_inv_%s.csv" % store_id
	print "hello: ", ftppath+filename

	readfile = simple_ftp.ftp_grab(
																	ftphost,
																	ftpport,
																	ftpuser,
																	ftppass,
																	ftppath,
																	filename
																)[0]
	df = pd.read_csv(readfile, dtype=str)
	df.fillna("not applicable", inplace=True)
	filelist = []
	for scraper in scrapers:
		filename = 'urlsfor_%s_%s.csv' % (scraper.replace('.',''), store)
		subdf = df[df['Competitor URL'].str.contains(sitenames[scrapers.index(scraper)])]
		urldf = pd.DataFrame({}, columns=["sku", "url"])
		urldf['sku'] = subdf["Inventory Number"]
		urldf['url'] = subdf["Competitor URL"]
		urldf.to_csv(filename, index=False)
		filelist.append(filename)
	print "uploading files:", filelist
	ftp_drop(filelist)
	for filename in filelist:
		scrapername = scrapers[filelist.index(filename)].replace('.','')
		hulk_upload_urls(store, filename, scrapername)
		sp.call("rm %s" % filename, shell=True)


