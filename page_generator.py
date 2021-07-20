#!/usr/bin/env python3

import random
import numpy.random
import string
import math

# generates a set of testing pages
#   - file_name: name of the file in which save the pages
#   - nPage: number of pages to be generated
#   - nOutLinks: mean value of outLinks per page
def generate_pages(file_name: str, nPage:int, nOutLinks: int):
    if nPage <= 0 or nOutLinks <= 0:
        return
    
    titles = []
    
    for i in range(nPage):
        while True:
            title = _generate_random_text(15)
            if title not in titles:
                titles.append(title)
                break
    
    with open(file_name, 'w') as out_file:
        for title in titles:
            page = "<title>" + title + "</title>" + _generate_random_text(math.floor(numpy.random.exponential(80))) + _generate_random_outLinks(titles, nOutLinks)
            out_file.write(repr(page)+'\n')

# generates a random text of defined length        
def _generate_random_text(length: int) -> str:
    return ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase) for _ in range(length))

# get a uniform title to be used as outLink
def _pick_random_link(titles: list) -> str:
    return titles[math.floor(numpy.random.uniform(0,len(titles)))]

# generates the page body(<text></text>)
def _generate_random_outLinks( titles:list, nOutLinks:int) -> str:
    content = "<text>"
    nLinks = math.floor(numpy.random.normal(loc=nOutLinks))
    for a in range(nLinks):
        content = content+_generate_random_text(math.floor(numpy.random.exponential(10))) + "[[" + _pick_random_link(titles) + "]]"
    content = content + "</text>"
    
    return content

        