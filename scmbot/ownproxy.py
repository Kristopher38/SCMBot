from .proxyfinder import ProxyFinder
from scrapy import signals
from scrapy.exceptions import CloseSpider
from proxybroker import Proxy
from .proxy import ProxyExtended
from datetime import datetime, timedelta
import random
import pdb
import logging
import time
import json

logger = logging.getLogger(__name__)

class OwnProxy(object):
	def __init__(self, crawler):
		self.init_proxies = crawler.settings.get('MIN_PROXY_INIT', 10)
		self.max_errors = crawler.settings.get('MAX_PROXY_ERRORS', 3)
		self.limit = crawler.settings.get('PROXY_LIMIT', 25)
		self.proxy_types = crawler.settings.get('PROXY_TYPES', ['HTTP'])
		self.hold_time = crawler.settings.get('PROXY_HOLD_TIME', 30)
		self.min_hold_time = crawler.settings.get('MIN_PROXY_HOLD_TIME', 2)
		#self.retry_words = crawler.settings.get('RETRY_WORDS', [])
		self.finder = ProxyFinder(types=self.proxy_types, limit=self.limit)
		
	@classmethod
	def from_crawler(cls, crawler):
		o = cls(crawler)
		crawler.signals.connect(o.spider_opened, signal=signals.spider_opened)
		crawler.signals.connect(o.spider_closed, signal=signals.spider_closed)
		return o
		
	def process_request(self, request, spider):
		try:
			random_proxy = self._get_random_proxy()
		except NoLeftInPoolError as e:
			raise CloseSpider(e.__str__())
		else:
			random_proxy.stat['requests'] += 1
			request.meta['proxy'] = "http://%s:%s" % (random_proxy.host, random_proxy.port)
			request.meta['proxy_obj'] = random_proxy
			
	def process_response(self, request, response, spider):
		if 'proxy_obj' in request.meta:
			proxy = request.meta['proxy_obj']
			proxy._runtimes.append(request.meta['download_latency'])
			proxy.stat['status'][response.status] += 1
			proxy.in_use = False
			
			if response.status != 200:
				logger.debug("(Failed request) Proxy used for %s was %s", request.url, request.meta['proxy'])
				if response.status in [302, 407] and self.finder.proxies.count(proxy) > 0:	# Proxy authentication required or redirection to info about restricted access
					logger.debug("Removing proxy requring authentication or restricting access (code %d): %s", response.status, proxy)
					self.finder.proxies.remove(proxy)
				if response.status in [429, 503]:
					logger.debug("Putting proxy %s on hold for %s seconds", proxy, str(self.hold_time))
					proxy.hold_until = datetime.now() + timedelta(seconds=self.hold_time)
			else:
				proxy.hold_until = datetime.now() + timedelta(seconds=self.min_hold_time)
				logger.debug("Proxy used for %s was %s", request.url, request.meta['proxy'])
				# if not self._is_valid(response.text):
					# if self.finder.proxies.count(proxy) > 0:
						# logger.debug("Removing proxy restricting access %s", proxy)
						# self.finder.proxies.remove(proxy)
					# logger.debug("Retrying denied request made with proxy %s" % request.meta['proxy_obj'] if 'proxy_obj' in request.meta else None)
					# retryreq = request.copy()
					# retryreq.dont_filter = True
					# return retryreq
		return response
	
	def process_exception(self, request, exception, spider):
		if 'proxy_obj' in request.meta:
			proxy = request.meta['proxy_obj']
			proxy.stat['errors'][exception.__class__] += 1
			if sum(proxy.stat['errors'].values()) >= self.max_errors and self.finder.proxies.count(proxy) > 0:
				logger.debug("Removing proxy exceeding max error count %s", proxy)
				self.finder.proxies.remove(proxy)
			logger.debug("Putting proxy %s on hold for %s seconds", proxy, str(self.hold_time))
			proxy.hold_until = datetime.now() + timedelta(seconds=self.hold_time)
			proxy.in_use = False
		
	def spider_opened(self, spider):
		self.finder.start()
		while len(self.finder.proxies) < self.init_proxies:
			self.finder.wait_for_proxy()
		
	def spider_closed(self, spider):
		logger.info("Dumping proxy stats:")
		for proxy in self.finder.proxies:
			logger.info("%s %s Average runtime: %s Error rate: %s", proxy, proxy.stat, proxy.avg_resp_time, proxy.error_rate)
		self.finder.stop()
		
	def _get_random_proxy(self):
		self.finder.update_proxies()
		
		if len(self.finder.proxies) > 0:
			for i, proxy in enumerate(self.finder.proxies):
				if not isinstance(proxy, ProxyExtended):
					self.finder.proxies[i] = ProxyExtended(proxy)
		else:
			raise NoLeftInPoolError("No proxies left in the proxy pool")

		now = datetime.now()
		self.finder.proxies = sorted(self.finder.proxies, key=lambda x: x.avg_resp_time, reverse=False)

		for proxy in filter(lambda x: x.hold_until < now and x.in_use == False, self.finder.proxies): # allow only if past time of holding to
			proxy.last_used = now
			proxy.in_use = True
			return proxy
		else:
			return min(self.finder.proxies, key=lambda x: x.hold_until)
			#raise OnHoldError("All proxies are on hold")
		
	# def _is_valid(self, response_text):
		# try:
			# json.loads(response_text)
		# except ValueError:
			# text = response_text.lower()
			# for word in self.retry_words:
				# if word in text:
					# return False
			# return True
		# else:
			# return True

class NoLeftInPoolError(RuntimeError):
	pass
	