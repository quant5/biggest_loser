import requests
from lxml import etree
from datetime import date, timedelta
from time import sleep
import logging
import json

import model


class WSJScraper(object):

    def __init__(self):
        self.url_format = 'http://online.wsj.com/mdc/public/page/2_3021-losecomp-loser-{yyyy}{mm}{dd}.html'
        self.start_date = date(2013, 1, 1)

    def find_latest_date(self):
        latest = model.get_latest_date()
        if latest:
            logger.info('latest date found: {}'.format(latest))
            return latest
        logger.info('no latest date found. defaulting to start date {}'.format(self.start_date))
        return None

    def scrape_wsj(self, date):
        root = self._get_root_or_holiday(date)
        if root is None:
            logger.warning('holiday found: {}'.format(date))
            return

        table = root.xpath('//table[@class="mdcTable"]')[0]
        for i in range(2, 102):
            try:
                row = table.xpath('.//tr[{}]'.format(i))[0]
            except IndexError:
                continue
            rank = row.xpath('.//td[1]/text()')[0]
            ticker = row.xpath('.//td[2]/a[1]/@href')[0].split('=')[-1]
            closing_price = row.xpath('.//td[3]/text()')[0].lstrip('$')
            change = row.xpath('.//td[4]/text()')[0]
            pct_change = row.xpath('.//td[5]/text()')[0]
            model.store_row(date, rank, ticker, closing_price, change, pct_change)

    def _get_root_or_holiday(self, date):
        yyyy = date.strftime('%Y')
        mm = date.strftime('%m')
        dd = date.strftime('%d')
        url_to_scrape = self.url_format.format(yyyy=yyyy, mm=mm, dd=dd)
        res = requests.get(url_to_scrape)
        parser = etree.HTMLParser()
        root = etree.fromstring(res.content, parser)
        try:
            root.xpath('//table[@class="mdcTable"]')[0]
        except IndexError:
            return None
        return root

    def get_next_day_price(self, ticker, loss_date, count):
        yahoo_query = ('https://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20yahoo.finance.historicaldata'
                       '%20where%20symbol%20%3D%20%22{}%22%20and%20startDate%20%3D%20%22{}%22%20and%20endDate%20%3D%20%22{}%22'
                       '&format=json&env=http%3A%2F%2Fdatatables.org%2Falltables.env')
        next_date = date.strftime(self._get_next_biz_day(loss_date), '%Y-%m-%d')
        loss_date = date.strftime(loss_date, '%Y-%m-%d')
        res = requests.get(yahoo_query.format(ticker, loss_date, next_date))
        results_dict = json.loads(res.content)
        try:
            model.save_next_day_loser(results_dict['query']['results']['quote'][1], 
                                  results_dict['query']['results']['quote'][0], count)
            logger.info('Stock info saved for ticker {}, id {}'.format(ticker, count))
        except (TypeError, KeyError):
            logger.warning('No stock info found for ticker {}, id {}'.format(ticker, count))
            model.save_next_day_loser({'Symbol': ticker, 'Date': loss_date}, 
                                      {'Symbol': ticker, 'Date': next_date}, count)

    def _get_next_biz_day(self, date):
        days = 1
        if date.weekday() == 4:
            days += 2
            while self._get_root_or_holiday(date + timedelta(days=days)) is None:
                days += 1
            return date + timedelta(days=days)
        while self._get_root_or_holiday(date + timedelta(days=days)) is None:
            days += 1
        return date + timedelta(days=days)


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    scraper = WSJScraper()
    d = scraper.find_latest_date() + timedelta(days=1) or scraper.start_date
    while d <= date.today():
        if d.weekday() < 5:
            scraper.scrape_wsj(d)
            sleep(5)
        d += timedelta(days=1)
    logger.info('done')

    tickers_and_dates = model.get_tickers_and_dates()
    for ticker, loss_date, count in tickers_and_dates:
        scraper.get_next_day_price(ticker, loss_date, count)
