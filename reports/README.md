
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

**Usage:**
```py
from reports import generate_report as gr
competitor_report_df = gr.top_competitor_format(STORE_ID)
filtered_comp_data = gr.get_competitor_data(1446251, filters={'competitors': ["staples.com", "office"], 'brands': ["zep"]})
```
Expects: store_id (int or intable string), optional inputs include: filters,

Returns: DataFrame (pandas) with sku and competitors in top comp format

Side-effect: Prints settings used to cli

**To obtain the standard export use:**
```
gr.top_competitor_report(STORE_ID)
```
**Standard export example using filters:**
```
gr.top_competitor_report(1446251, filters={'competitors': ['staples.com']})
```

