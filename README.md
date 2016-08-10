**This is a collection of random (mostly python) modules used for TAM and Analyst work at Wiser**

**To use python better, get started and look at the usage guidlines for each module down below.**

For more information see our [WIKI](https://github.com/adamrdavid/python-tools/wiki)

Getting Started
===============

1. Clone the repo 

2. In each folder you want to use, add credentials to config.example.py and save as 'config.py'

3. Enter the top directory and run `python setup.py install`

4. ???

5. Profit

    


# Reports

###  Settings

report_settings.py has different functions to grab current report settings based on store_id and/or user_id

Usage:
```py
from reports import report_settings
custom_columns = report_settings.get_custom_columns(STORE_ID)
```
Expects: wiser_store_id

Returns: List of custom column names

===

`get_user(store_id)`

Returns: user_id associated with given store_id

===

`check_ftp_user(uid)`

Expects: wiser user id

Returns: True if ftp user exists, else False

===

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

**returns list of filenames found and transfers matching remote files to local dir**

`ftp_drop(host, port, user, pwrd, path, filename)`

`sftp_drop(host, port, user, pwrd, path, filename)`

**returns nothing but sends local file to remote path**

*Both grab functions use unix search to transfer all matching remote files to local dir and returns*

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

Usage:
```py
from reports import generate_report as gr
filters={'brands': ["dwalt"], 'competitors': ["staples.com"]}
gr.get_competitor_data(12345, filters)
```
===

**Return dataframe of competitor data in top competitor format**

`top_competitor_format(store_id, filters={}, screenshots=False, dedup=False)`

Usage:
```py
from reports import generate_report as gr
gr.top_competitor_format(12345, screenshots=True)
```

===

**Return dataframe of the standard legacy report in top comp format**

`top_competitor_report(store_id, filters={}, custom_columns=True, screenshots=False, dedup=False, format_headers=False)`

Usage:
```py
from reports import generate_report as gr
gr.top_competitor_report(12345, screenshots=True, format_headers=True)
```
* format_headers = True will give you headers just like the legacy report

===

**Return dataframe of the legacy report in distinct rows**

`distinct_row_report(store_id, filters={}, custom_columns=True, screenshots=False, dedup=False)`

===

## Custom Querying

**To Perform a Custom Query on the DB involving 'competitor data':**

```py
from reports import generate_report as gr
columns = [
  {
    'table':'products',
    'column':'name',
    'name':'product name'
  },
  {
    'table':'pricing',
    'column':'store_name',
    'name':'comp name'
  },
  {
    'table':'products',
    'column':'sku',
    'name':'sku num'
  },
  {
    'table':'pricing',
    'column':'price',
    'name':'comp price'
  },
  {
    'table':'pricing',
    'column':'ship',
    'name':'comp shipping'
  },
  {
    'table':'product_labels',
    'column':'keyword',
    'name':'labelers'
  },
  {
    'table':'pps_custom_attributes',
    'column':'num_size',
    'name':'product size'
  },
  {
    'table':'map_violators_screenshots',
    'column':'image_url',
    'name':'screenshot'
  }
]
filters = {'competitors': ['ezcontacts']}
print gr.query_competitor_data(1178770744, columns=columns, filters=filters).head()
```

## Hulk Stuff

*Run automated tasks on hulk via python scripts*

#### Uploading competitor prices (also used for custom repricing rules)

`upload_prices(file_path, store_id, comp_id)`

Usage:

```py
from hulk import upload_prices as up

up.upload_prices('files/upload_prices_test_file.csv', 1196234460, 1197010905)
```

#### SSH

#### Manual Scrape

#### Aggregate Google Shopping URLs



## Scraping Skrimps

Python scraping template examples

Need to add examples for:
- Proxies
- Translate
- Screenshots
- Validations
- Search from inputs: db, csv, json
