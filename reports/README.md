
# Reports

###  Settings

report_settings.py has different functions to grab current report settings based on store_id and user_id

Usage:
```py
from reports import report_settings
custom_columns = report_settings.get_custom_columns(STORE_ID)
```
Expects: wiser_store_id

Returns: List of custom column names

### Fun URL

fun_url.py will help you clean up those long and ugly urls

Usage:

`fun_url.split_url(URL)` will split a redirect url and return the target url contained inside. (supports clickserve and mercent)

`fun_url.shorten_url(URL)` will use the google url shortening api to return a goo.gl (shortened) url

`fun_url.clean_url(URL)` will call split_url, then if the url is >255 characters it will call shorten_url

###  Simple FTP

simple_ftp.py has functions for different tasks using ftp/sftp/ftps (supports key based auth with paramiko)

`ftp_grab(host, port, user, pwrd, path, search_keyword)`

`sftp_grab(host, port, user, pwrd, path, search_keyword)`

returns list of filenames found and transfers matching remote files to local dir

`ftp_drop(host, port, user, pwrd, path, filename)`

`sftp_drop(host, port, user, pwrd, path, filename)`

returns nothing but sends local file to remote path

Both grab functions use unix search to transfer all matching remote files to local dir and returns

see: http://www.codecoffee.com/tipsforlinux/articles/26-1.html for help with search_keyword


###  Email

send_email.py lets you send emails with attachments (based on smtplib and mimetypes)
`send_email(email_subject, filename_list, email_list, bcc_list=[])`

Params:
```
email_subject: string - email subject line

filename_list: list of strings - files to be attached

email_list: lsit of strings - email addresses in recipient list

bcc_list: list of strings - email addresses hidden from recipient list
```

Returns: nothing

**Usage:**
```py
from reports import send_email as se
email_list = ['client1@client.com', 'client2@client.com']
bcc_list = ['adam.david@wiser.com', 'thumarut.vareechon@wiser.com', 'tenzin.wangdhen@wiser.com']
file_list = ['MAP_report1.csv', 'MAP_report2.csv']
se.send_email("WiseReport - MAP Policy Updated Violators", file_list, email_list, bcc_list)
```

## Generating Reports

generate_report.py lets you create report templates with different strategies (may need massaging)

**Return store settings for competitor reports**

`get_comp_settings(store_id)`

===

**Return dataframe of raw product data in standard format**

`get_product_data(store_id, filters={})`

===

**Return dataframe of all custom columns**

`get_custom_column_data(store_id)`

===

**Return dataframe of competitor data in standard row format**

`get_competitor_data(store_id, filters={}, dedup=False)`

===

**Return dataframe of competitor data in top competitor format**

`top_competitor_format(store_id, filters={}, screenshots=False, dedup=False)`

===

**Return dataframe of the standard legacy report in top comp format**

`top_competitor_report(store_id, filters={}, custom_columns=True, screenshots=False, dedup=False, format_headers=False)`

===

