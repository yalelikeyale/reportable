import os
import sys
import time
import pysftp
import MySQLdb
import subprocess as sp
from ftplib import FTP

# constants
wiser_host = "ftp.wiser.com"
wiser_port = 21
wiser_user = os.environ['FTP_USER']
wiser_pass = os.environ['FTP_PASS']
# cmd line variables
job_type = sys.argv[1]
connection_type = sys.argv[2]
user_name = sys.argv[3]
ftp_host = sys.argv[4]
ftp_port = sys.argv[5]
ftp_user = sys.argv[6]
ftp_pass = sys.argv[7]
ftp_path = sys.argv[8]
search_keyword = sys.argv[9]

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
			sftp.put("./"+filename,remotepath)
		except Exception as e:
			print e
		else:
			print time.strftime("%c"), "sftp drop: %s" % remotepath
			print "Success!"
		sftp.close()


### MAIN ###
print "len", len(sys.argv)
print sys.argv
## import job
if job_type == 'import':
    grab_path = ftp_path
    drop_path = "usersftp/LocalUser/%s/Upload" % user_name
    if connection_type == 'sftp - no key':
		filelist = sftp_grab(ftp_host, ftp_port, ftp_user, ftp_pass, grab_path, search_keyword)
		for filename in filelist:
			try:
				ftp_drop(wiser_host, wiser_port, wiser_user, wiser_pass, drop_path, filename)
			except Exception as e:
				print time.strftime("%c"),"error:",e
		print "Success: transfer complete!"
    elif connection_type == 'sftp - with key':
        print "ffs FUCK OFF now!"
    elif connection_type == 'ftps':
        print 'nopersnopitynope'
    elif connection_type == 'ftp':
		filelist = ftp_grab(ftp_host, ftp_port, ftp_user, ftp_pass, grab_path, search_keyword)
		for filename in filelist:
			try:
				ftp_drop(wiser_host, wiser_port, wiser_user, wiser_pass, drop_path, filename)
			except Exception as e:
				print time.strftime("%c"),"error:",e
			else:
				output = 1
				print "Success: transfer complete!"
    else:
		# should not get here, invalid connection type
		print "ya done fucked up junior!"
## export job
elif job_type == 'export':
	grab_path = "usersftp/LocalUser/%s/Download" % user_name
	drop_path = ftp_path
	if connection_type == "sftp - no key":
		filelist = ftp_grab(wiser_host, wiser_port, wiser_user, wiser_pass, grab_path, search_keyword)
		for filename in filelist:
			try:
				sftp_drop(ftp_host, ftp_port, ftp_user, ftp_pass, drop_path, filename)
			except Exception as e:
				print time.strftime("%c"),"error:",e
		print "Success: transfer complete!"
	elif connection_type == 'sftp - with key':
		print "ffs FUCK OFF now!"
	elif connection_type == 'ftps':
		print 'nopersnopitynope'
	elif connection_type == 'ftp':
		filelist = ftp_grab(wiser_host, wiser_port, wiser_user, wiser_pass, grab_path, search_keyword)
		for filename in filelist:
			try:
				ftp_drop(ftp_host, ftp_port, ftp_user, ftp_pass, drop_path, filename)
			except Exception as e:
				print time.strftime("%c"),"error:",e
		print "Success: transfer complete!"
	else:
		# should not be possible, invalid connection type
		print "ya done fucked up junior!"

if filelist:
	print "delesting old files:", filelist
	for filename in filelist:
		print time.strftime("%c"),"removing:",filename
		os.remove(filename)
print "All Done!"

