# -*- coding: utf-8 -*-
import scrapy
import json
import re
from datetime import datetime
import matplotlib.pyplot as plt
from . import fees
import pdb

class ItemcrawlerSpider(scrapy.Spider):
	name = "itemcrawler"
	
	allowed_domains = ['steamcommunity.com']
	custom_settings = {
		'RETRY_HTTP_CODES': [500, 503, 504, 400, 403, 404, 407, 408, 429],
		'RETRY_TIMES': 10,
		'DOWNLOADER_MIDDLEWARES': {
			'scmbot.ownproxy.OwnProxy': 555,
			'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 556,
			'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None
		},
		'USER_AGENT': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
		'DOWNLOAD_TIMEOUT': 15,
		'RANDOM_UA_PER_PROXY': True
		#'CONCURRENT_REQUESTS': 1,
		#'DOWNLOAD_DELAY': 1.5
	}
	
	links = {
		'search':'http://steamcommunity.com/market/search/render/?query={query}&start={start}&count={count}&sort_column={sort_column}&sort_dir={sort_dir}&appid={appid}',
		'priceoverview': 'http://steamcommunity.com/market/priceoverview/?appid={appid}&country={country}&currency={currency}&market_hash_name={market_hash_name}'
	}
	regexps = {
		'search': 'market_listing_row_link.+href="(.+\/(.+?))\?',
		'listings': 'var line1=(\[.+\]);'
	}
	
	volume_threshold = 0
	days_to_calc_extrema = 10
	profit_threshold = -9999999
	
	# TODO: Subclassed proxybroker with ability to queue jobs for grabbing proxies, pausing grabbing when done job, and queues in ProxyFinder to allow for that
	# TODO: Improve proxy selection algorithm to faciliate response time (or maybe not??)
	# TODO: Refactor requests so parameters aren't hardcoded (todo: appid passing from appid in start_requests to appid in parse_search so it doesn't trigger internal server error)
	
	def start_requests(self):
		url = self.links['search'].format(query="trading card", start="0", count="10", sort_column="quantity", sort_dir="desc", appid="753")
		yield scrapy.Request(url=url, callback=self.parse_search)

	def parse_search(self, response):
		if self.is_json(response.text):
			#formatted = response.text.decode('string_escape').replace("\/", "/")
			data = json.loads(response.text)
			for item_url, item_name in re.findall(self.regexps['search'], data['results_html']):
				priceoverview_url = self.links['priceoverview'].format(appid="753", country="PL", currency="3", market_hash_name=item_name)
				meta = {'item_url': item_url, 'item_name': item_name}
				yield scrapy.Request(url=priceoverview_url, callback=self.parse_priceoverview, meta=meta)
		
	def parse_priceoverview(self, response):
		if self.is_json(response.text):
			data = json.loads(response.text)
			if data['success']:
				if int(data['volume'].replace(",","")) >= self.volume_threshold:
					meta = {'volume': data['volume']}
					yield scrapy.Request(url=response.meta['item_url'], callback=self.parse_item, meta=meta)
			else:
				yield response.request.copy()
		
	def parse_item(self, response):
		if self.is_json(response.text):
			chart_data = json.loads(re.search(self.regexps['listings'], response.text).group(1))
			for i, entry in enumerate(chart_data):
				date, price, quantity = entry
				datetime_object = datetime.strptime(date, "%b %d %Y %H: +0")
				quantity_int = int(quantity)
				chart_data[i] = [datetime_object, price, quantity_int]
			processed_chart_data = self.process_chart(chart_data)
			if processed_chart_data:
				processed_chart_data.update({'url': response.url, 'volume': response.meta['volume']})
				yield processed_chart_data
		
	def is_json(self, text):
		try:
			json_obj = json_loads(text)
		except ValueError:
			self.logger.error("Text is not a valid json: %s", text)
			return False
		else:
			return True
		
	def split_chart_to_days(self, chart):
		temp_chart = []
		start_index = 0
		current_date = chart[0][0].date()
		for i, chart_data in enumerate(chart):
			date, price, quantity = chart_data
			if current_date != date.date():
				temp_chart.append(chart[start_index:i])
				start_index = i
				current_date = date.date()
		temp_chart.append(chart[start_index:])
		return temp_chart
		
	def split_chart_to_lists(self, chart):
		dates = []
		prices = []
		quantities = []
		for chart_day in chart:
			for i, chart_data in enumerate(chart_day):
				date, price, quantity = chart_data
				dates.append(date)
				prices.append(price)
				quantities.append(quantity)
		return (dates, prices, quantities)
		
	def find_minmax(self, chart_day):
		min = chart_day[0][1]
		max = chart_day[0][1]
		for date, price, quantity in chart_day:
			if price < min:
				min = price
			if price > max:
				max = price
		return (min, max)
		
	def process_chart(self, chart):
		chart = self.split_chart_to_days(chart)
		chart = chart[-self.days_to_calc_extrema:]
		
		max_sum = 0
		min_sum = 0
		for chart_day in chart:
			min, max = self.find_minmax(chart_day)
			min_sum += min
			max_sum += max
		avg_min = min_sum/self.days_to_calc_extrema
		avg_max = max_sum/self.days_to_calc_extrema
		
		max_price = int(round(avg_max*100))
		min_price = int(round(avg_min*100))
		
		fee = fees.calculate_fee_amount(max_price, 0.1)
		i_get_sell = max_price - fee['fees']
		
		expected_profit = i_get_sell - min_price 
		if expected_profit >= self.profit_threshold:
			return {
				"price_perc": round(float(expected_profit)/float(min_price)*1000)/1000, 
				"avg_min": avg_min, 
				"avg_max": avg_max, 
				"sell_price_fee": max_price,
				"sell_price_nofee": i_get_sell,
				"expected_profit": expected_profit}
		else:
			return False
				
		# dates, prices, quantities = self.split_chart_to_lists(chart)
		# plt.plot(dates, prices)
		# plt.show()
		

	def parse(self, response):
		pass