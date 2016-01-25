import os
import sys
import time
import pysftp
import MySQLdb
import subprocess as sp
from ftplib import FTP
from config import ftp_creds

# constants
wiser_host = ftp_creds['host']
wiser_port = ftp_creds['port']
wiser_user = ftp_creds['username']
wiser_pass = ftp_creds['password']

# job_type = "export"
# connection_type = "sftp - no key"
# user_name = "User5542"
# ftp_host = ''
# ftp_port = 2222
# ftp_user = 'wiser'
# ftp_pass = ''
# ftp_path = "in"
# search_keyword = ""
output = 0
#normalize
# if ftp_pass == "blank":
# 	ftp_pass = ""
# if search_keyword == "blank":
# 	search_keyword = ""


# grab file(s) from ftp
def ftp_grab(host, port, user, pwrd, path, search_keyword):
	ftp = FTP()
	ftp.connect(host, port)
	ftp.login(user, pwrd)
	print "logged in"
	ftp.cwd(path)
	filelist = ftp.nlst(str(search_keyword))
	print filelist
	for readfile in filelist:
		print "grabbing...", readfile
		try:
			with open(readfile, 'wb') as f:
				ftp.retrbinary('RETR %s' % readfile, f.write)
		except Exception as e:
			print time.strftime("%c"),"error:",e
		else:
			print time.strftime("%c"),"grabbed:",readfile
	return filelist

# drop one file into ftp
def ftp_drop(host, port, user, pwrd, path, filename):
	filename = str(filename)
	ftp = FTP()
	ftp.connect(host, port)
	ftp.login(user, pwrd)
	print "logged in"
	print "path is:",path
	print "file is:",filename
	ftp.cwd(path+"/")
	print "cwd successfull"
	ftp.storbinary('STOR '+filename, open(filename, 'rb'))
	print time.strftime("%c"), "ftp drop: ", filename

# grab file from remote sftp
def sftp_grab(host, port, user, pwrd, path, search_keyword):
	with pysftp.Connection(host=host, port=int(port), username=user, password=pwrd) as sftp:
		filelist = sftp.listdir(remotepath=path) # list files in remote path
		print filelist
		for readfile in filelist:
			readfile = str(readfile)
			remotepath = path+"/"+readfile
			print "grabbing..."
			try:
				sftp.get(remotepath=remotepath)  # download file from remotepath
				print "got"
			except Exception as e:
				print time.strftime("%c"),"error:",e
	    	else:
	    		print time.strftime("%c"),"grabbed:",readfile
        return filelist

# drop file in remote sftp
def sftp_drop(host, port, user, pwrd, path, filename):
	with pysftp.Connection(host=host, port=int(port), username=user, password=pwrd) as sftp:
		remotepath = path+"/"+filename
		print "remote path:", remotepath
		# increase window size for large uploads
		channel = sftp.sftp_client.get_channel()
		channel.lock.acquire()
		print "lock acquired, changing window size"
		channel.out_window_size += os.stat(filename).st_size
		channel.out_buffer_cv.notifyAll()
		print "buffering"
		channel.lock.release()
		print "lock released"
		# upload file to remotepath
		try:
			sftp.put(filename,remotepath)
		except Exception as e:
			print e
		else:
			print time.strftime("%c"), "sftp drop: %s" % remotepath
			print "Success!"
		sftp.close()
