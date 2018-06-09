# !/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-06-04 09:45:29
# Project: InvestChile

from pyspider.libs.base_handler import *
import re
import time
import pymongo


class Handler(BaseHandler):
    crawl_config = {
    }

    client = pymongo.MongoClient('localhost')
    db = client['pdf_info']

    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl('https://investchile.gob.cl/publications/', callback=self.index_page,
                   validate_cert=False)

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        next_page = response.doc('a.nextpostslink').attr.href
        print(next_page)
        if next_page:
            self.crawl(next_page, callback=self.index_page, validate_cert=False)

        results = []
        for item_event in response.doc('div.item-evento').items():
            temp = item_event('div.datos-evento').text()
            if 'PDF' in temp:
                title = item_event('div.contenido-evento h4 a').text()
                date = re.search('Publicado el(.*?)/ PDF', temp).group(1).strip()
                format_time = time.strptime(date, '%d %B, %Y')
                url = item_event('div.contenido-evento h4 a').attr.href

                result = {
                    'pdf_url': url,
                    'pdf_title': title,
                    'pub_time': time.strftime('%Y-%m-%d %X', format_time),
                    'save_time': time.strftime('%Y-%m-%d %X', time.localtime())
                }
                print(result)
                results.append(result)
        print(results)
        return results

    def on_result(self, result):
        if result:
            self.save_mongo(result)

    def save_mongo(self, result):
        if self.db['InvestChile'].insert_many(result):
            print('succeed to save MongoDB')
