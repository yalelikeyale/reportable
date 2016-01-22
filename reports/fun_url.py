import json
import requests
from config import shorturl_api_key

max_length = 255

def split_url(url):
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
    r = requests.post(post_url, data=json.dumps(payload), headers=headers)
  except Exception, e:
    print e
  else:
    url = str(json.loads(r.text)['id'])
  return url


def clean_url(url):
  url = split_url(url)
  if len(url) > max_length:
    url = shorten_url(url)
  return url


