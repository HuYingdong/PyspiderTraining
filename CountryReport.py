#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-06-04 16:43:01
# Project: CountryReport
import pymongo
from pyspider.libs.base_handler import *
import re
import json
from pyspider.libs.utils import md5string
import pdfkit
import os
import time


class Handler(BaseHandler):
    crawl_config = {
    }

    client = pymongo.MongoClient('localhost')
    db = client['pdf_info']

    def get_taskid(self, task):
        return md5string(task['url'] + json.dumps(task['fetch'].get('data', '')))

    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl('https://countryreport.mofcom.gov.cn/record/index110209.asp', callback=self.all_pages,
                   validate_cert=False)

    def all_pages(self, response):
        page_info = response.doc('div.page').text()

        total_page = re.search('/总(\d+)页', page_info).group(1)
        print('total pages are: ', total_page)

        for page in range(int(total_page)):
            self.crawl(response.url, callback=self.index_page, method='POST', data={'me_page': page},
                       validate_cert=False)

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        response.raise_for_status()
        for each_dl in response.doc('dl.dl01 dd').items():
            title = each_dl('a').text()
            if '/' in title:
                title = title.replace('/', '_')
            elif ':' in title:
                title = title.replace(':', '_')

            url = each_dl('a').attr.href
            date = each_dl('span').text()
            print(title, date, url)
            self.crawl(url, callback=self.detail_page, validate_cert=False, save={
                'pub_time': date,
                'report_title': title
            })

    @config(priority=2)
    def detail_page(self, response):
        response.raise_for_status()
        detail_url = response.doc('div.adr h3 a').attr.href
        print(detail_url)
        self.crawl(detail_url, callback=self.pdf_page, validate_cert=False, save=response.save)

    def pdf_page(self, response):
        response.raise_for_status()
        pdf_lst = []
        for li in response.doc('ul.t1 li').items():
            pdf_lst.append(li('a').attr.href)

        filepath = os.getcwd() + '\CountryReport\\'
        if not os.path.exists(filepath):
            os.mkdir(filepath)

        filename = filepath + response.save['report_title'] + '.pdf'
        pdfkit.from_url(pdf_lst, filename, options={'quiet': ''})

        tm = time.strptime(response.save['pub_time'], '%Y-%m-%d')
        pub_time = time.strftime('%Y-%m-%d %X', tm)

        result = {
            'report_title': response.save['report_title'],
            'report_url': response.url,
            'pdf-path': filename,
            'pub_time': pub_time,
            'save_time': time.strftime('%Y-%m-%d %X', time.localtime())
        }
        print(result)
        return result

    def on_result(self, result):
        if result:
            self.save_mongo(result)

    def save_mongo(self, result):
        if self.db['CountryReport'].insert_one(result):
            print('succeed to save MongoDB: ', result['report_title'])
