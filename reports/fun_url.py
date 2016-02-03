import json
import time
import urllib
import requests
from config import shorturl_api_key

rate_limit = 1
max_length = 255

def split_url(url):
  # split remaining url if needed
  if "link.mercent.com" in url:
    if "3fcm_mmc" in url:
      try: 
        url = str(url).split('targetUrl=')[1].split("3fcm_mmc")[0].replace('%', '')
      except Exception, e:
        print e
    elif "3Fcm_mmc":
      try: 
        url = str(url).split('targetUrl=')[1].split("3Fcm_mmc")[0][:-1]
      except Exception, e:
        print e
  if "clickserve.dartsearch.net" in url:
    try:
      url = str(url).split('dest_url=')[1]
    except Exception, e: 
      print e
  if 'shopbop' in url:
    print "SHOPBOP shopbop URL url: ", url
    try:
      url = str(url).split('?currency')[0]
    except Exception, e:
      print e

  return url

def shorten_url(url):
  post_url = 'https://www.googleapis.com/urlshortener/v1/url?key=%s' % shorturl_api_key
  payload = {'longUrl': url}
  headers = {'content-type': 'application/json'}
  try:
    #url = urllib.quote(url)
    r = requests.post(post_url, data=json.dumps(payload), headers=headers)
    #time.sleep(rate_limit)
  except Exception, e:
    print e
  else:
    #print r.text
    try:
      url = str(json.loads(r.text)['id'])
    except Exception, e:
      print "Failed to shorten this url: ", url
      print "Result from failed request: ", r.text
      print "Exception: ", e
  return url


def clean_url(url):
  url = split_url(url)
  if len(url) > max_length:
    # try to grab redirect useing urllib
    url = urllib.urlopen(str(url))
    url = url.geturl()
    print "url shortened with urllib: ", url
  if len(url) > max_length:
    url = shorten_url(url)
    print "url shortened with google api: ", url
  return url


