from .proxyfinder import ProxyFinder
from scrapy import signals
from proxybroker import Proxy
from .proxy import ProxyExtended
from datetime import datetime, timedelta
import random
import pdb
import logging

logger = logging.getLogger(__name__)

class OwnProxy(object):
	def __init__(self, crawler):
		self.init_proxies = crawler.settings.get('MIN_PROXY_INIT', 10)
		self.max_errors = crawler.settings.get('MAX_PROXY_ERRORS', 3)
		self.limit = crawler.settings.get('PROXY_LIMIT', 25)
		self.proxy_types = crawler.settings.get('PROXY_TYPES', ['HTTP'])
		self.hold_time = crawler.settings.get('PROXY_HOLD_TIME', 30)
		self.finder = ProxyFinder(types=self.proxy_types, limit=self.limit)
		self.proxies = self.finder.proxies
		
	@classmethod
	def from_crawler(cls, crawler):
		o = cls(crawler)
		crawler.signals.connect(o.spider_opened, signal=signals.spider_opened)
		crawler.signals.connect(o.spider_closed, signal=signals.spider_closed)
		return o
		
	def process_request(self, request, spider):
		random_proxy = self._get_random_proxy()
		random_proxy.stat['requests'] += 1
		request.meta['proxy'] = "http://%s:%s" % (random_proxy.host, random_proxy.port)
		request.meta['proxy_obj'] = random_proxy
			
	def process_response(self, request, response, spider):
		if 'proxy_obj' in request.meta:
			proxy = request.meta['proxy_obj']
			proxy._runtimes.append(request.meta['download_latency'])
			proxy.stat['status'][response.status] += 1
			if response.status != 200:
				if response.status == 407 and self.proxies.count(proxy) > 0:	# Proxy authentication required
					logger.debug("Removing proxy requring authentication %s", proxy)
					self.proxies.remove(proxy)
				logger.debug("(Failed request) Proxy used for %s was %s", request.url, request.meta['proxy'])
				if response.status == 429:
					logger.debug("Putting proxy %s on hold for %s seconds", proxy, str(self.hold_time))
					proxy.hold_until = datetime.now() + timedelta(seconds=self.hold_time)
			else:
				logger.debug("Proxy used for %s was %s", request.url, request.meta['proxy'])
		return response
	
	def process_exception(self, request, exception, spider):
		if 'proxy_obj' in request.meta:
			proxy = request.meta['proxy_obj']
			proxy.stat['errors'][exception.__class__] += 1
			if sum(proxy.stat['errors'].values()) >= self.max_errors and self.proxies.count(proxy) > 0:
				logger.debug("Removing proxy exceeding max error count %s", proxy)
				self.proxies.remove(proxy)
		
	def spider_opened(self, spider):
		self.finder.start()
		while len(self.proxies) < self.init_proxies:
			self.finder.wait_for_proxy()
		
	def spider_closed(self, spider):
		logger.info("Dumping proxy stats:")
		for proxy in self.proxies:
			logger.debug("%s %s Average runtime: %s Error rate: %s", proxy, proxy.stat, proxy.avg_resp_time, proxy.error_rate)
		self.finder.stop()
		
	def _get_random_proxy(self):
		self.finder.update_proxies()
		if len(self.proxies) > 0:
			for i, proxy in enumerate(self.proxies):
				if not isinstance(proxy, ProxyExtended):
					self.proxies[i] = ProxyExtended(proxy)
			newlist = sorted(self.proxies, key=lambda x: x.last_used, reverse=False)
			for proxy in newlist:
				if proxy.hold_until < datetime.now():	# allow only if we're past the point in time till holding
					proxy.last_used = datetime.now()
					return proxy
			else:
				raise RuntimeError("All proxies are on hold")
		else:
			raise RuntimeError("No proxies left in the proxy pool")