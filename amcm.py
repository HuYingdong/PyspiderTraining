#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-06-05 16:44:39
# Project: amcm
import re
from pyspider.libs.base_handler import *
import time
import pymongo


class Handler(BaseHandler):
    crawl_config = {
    }

    client = pymongo.MongoClient('localhost')
    db = client['pdf_info']

    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl('http://www.amcm.gov.mo/zh/research-statistics/research-and-publications',
                   callback=self.index_page)

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        for category in response.doc('div.a-link a').items():
            category_title = category.text()
            if ('月報' not in category_title) and ('特刊' not in category_title):
                category_url = category.attr.href
                print(category_title, category_url)
                self.crawl(category_url, callback=self.category_page)

    @config(priority=2)
    def category_page(self, response):
        results = []
        years_lst = []
        items_lst = []

        years = response.doc('article h2.gby').items()
        reports = response.doc('article div.row').items()
        for year in years:
            years_lst.append(year.text())
        for report in reports:
            items_lst.append(report.find('a').items())

        if years_lst and items_lst:
            print(years_lst)
            print(items_lst)
            assert len(years_lst) == len(items_lst)

            for i in range(len(years_lst)):
                for report in items_lst[i]:
                    current_year = years_lst[i]
                    title = report.parent().text().strip()
                    report_title = current_year + '年' + title.replace('\n', '')
                    report_url = report.attr.href
                    if '.pdf' in report_url:
                        result = {
                            'report_title': report_title,
                            'report_url': report_url,
                            'pub_time': self.format_time(report_title),
                            'save_time': time.strftime('%Y-%m-%d %X', time.localtime())
                        }
                        print(result)
                        results.append(result)
                    else:
                        self.crawl(report_url, callback=self.detail_page, save={
                            'pub_time': self.format_time(report_title)})

        else:
            reports = response.doc('.attachment p a').items()
            for report in reports:
                title = report.text()
                url = report.attr.href
                result = {
                    'report_title': title,
                    'report_url': url,
                    'pub_time': self.format_time(title),
                    'save_time': time.strftime('%Y-%m-%d %X', time.localtime())
                }
                print(result)
                results.append(result)

        return results

    def detail_page(self, response):
        results = []
        items = response.doc('div.attachment p a').items()
        for item in items:
            url = item.attr.href
            title = item.text()
            result = {
                'report_title': title,
                'report_url': url,
                'pub_time': response.save['pub_time'],
                'save_time': time.strftime('%Y-%m-%d %X', time.localtime())
            }
            print(result)
            results.append(result)
        return results

    def on_result(self, result):
        if result:
            self.save_mongo(result)

    def save_mongo(self, result):
        if self.db['AmPublctn'].insert_many(result):
            print('succeed to save MongoDB')

    def format_time(self, date):
        str_months = list('一二三四五六七八九十') + ['十一', '十二']
        num_months = [str(i) for i in range(1, 13)]
        months = {str_months[i]: num_months[i] for i in range(12)}

        str_days = list('一二三四五六七八九十') + ['十' + i for i in '一二三四五六七八九'] + \
                   ['二十'] + ['二十' + i for i in '一二三四五六七八九'] + ['三十', '三十一']
        num_days = [str(i) for i in range(1, 32)]
        days = {str_days[i]: num_days[i] for i in range(31)}

        date_str = re.search('^(\d+)年(.*?)月(.*?)日$', date)
        if date_str:
            num_year = date_str.group(1)
            num_month = months.get(date_str.group(2))
            num_day = days.get(date_str.group(3))
            time_str = num_year + '-' + num_month + '-' + num_day
            return time.strftime('%Y-%m-%d %X', time.strptime(time_str, '%Y-%m-%d'))
        else:
            date_str = re.search('^(\d+)年(.*?)月', date)
            if date_str:
                num_year = date_str.group(1)
                num_month = months.get(date_str.group(2))
                time_str = num_year + '-' + num_month
                return time.strftime('%Y-%m-%d %X', time.strptime(time_str, '%Y-%m'))
            else:
                return None

