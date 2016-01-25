
## Reports

####  Settings

report_settings.py has different functions to grab current report settings based on store_id and user_id

Usage:
```py
from reports import report_settings
custom_columns = report_settings.get_custom_columns(STORE_ID)
```
Expects: wiser_store_id

Returns: List of custom column names

#### Fun URL

fun_url.py will help you clean up those long and ugly urls

Usage:

`fun_url.split_url(URL)` will split a redirect url and return the target url contained inside. (supports clickserve and mercent)

`fun_url.shorten_url(URL)` will use the google url shortening api to return a goo.gl (shortened) url

`fun_url.clean_url(URL)` will call split_url, then if the url is >255 characters it will call shorten_url

####  Simple FTP

simple_ftp.py has functions for different tasks using ftp/sftp/ftps (supports key based auth with paramiko)

####  Email

send_email.py lets you send emails with attatchments (based on smtplib and mimetypes)

###  Generating Reports

generate_report.py lets you create report templates with different strategies (may need massaging)

Usage:
```py
from reports import generate_report
competitor_data = generate_report.top_competitor_format(STORE_ID, USER_ID)
```
Expects: store_id and user_id (int or intable string)

Returns: DataFrame (pandas) with sku and competitors in top comp format

Side-effect: Prints settings used to cli
