#
# liburdi.joshua@gmail.com

import requests
import socket
import random
import lxml.html
import json
import sys

def get_user_agent(user_agents):
    return random.choice(user_agents)

def get_referer(referers):
    return random.choice(referers)

if __name__=='__main__':
    data = []
    domain_search = sys.argv[-1]
    ultratools = 'https://www.ultratools.com/tools/asnInfoResult?domainName='
    ultratools += domain_search
    ripe = 'https://stat.ripe.net/data/announced-prefixes/data.json?preferred_version=1.1&resource='
    user_agents = ['Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/601.2.7 (KHTML, like Gecko) Version/9.0.1 Safari/601.2.7', 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36', 'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko']
    referers = ['http://www.google.com', 'http://www.yahoo.com',
    'http://www.bing.com']
    user_agent = get_user_agent(user_agents)
    referer = get_referer(referers)
    headers = { 'User-Agent': user_agent, 'Referer': referer}

    ultra_page = requests.get(ultratools,headers=headers)
    ultra_tree = lxml.html.fromstring(ultra_page.text)
    asn_cache = ultra_tree.xpath('//div[@class="tool-results-heading"]/text()')

    ripe_cache = []

    for asn in asn_cache:
        ripe_search = ripe + asn
        ripe_page = requests.get(ripe_search,headers=headers)
        ripe_json = json.loads(ripe_page.text)
        for entry in ripe_json['data']['prefixes']:
            ripe_cache.append(entry['prefix'])


    ripe_set = set(ripe_cache)
    ripe_list = list(ripe_set)
    for entry in ripe_list:
        print entry
