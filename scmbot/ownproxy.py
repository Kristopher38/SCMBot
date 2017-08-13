from .proxyfinder import ProxyFinder
from scrapy import signals
import random
import pdb
import logging

logger = logging.getLogger(__name__)

class OwnProxy(object):
	def __init__(self, crawler):
		self.finder = ProxyFinder(types=[('HTTP', ('Anonymous', 'High'))], limit=25)
		self.proxies = self.finder.proxies
		self.init_proxies = crawler.settings.get('MIN_PROXY_INIT', 10)
		self.max_errors = crawler.settings.get('MAX_PROXY_ERRORS', 3)
		
	@classmethod
	def from_crawler(cls, crawler):
		o = cls(crawler)
		crawler.signals.connect(o.spider_opened, signal=signals.spider_opened)
		crawler.signals.connect(o.spider_closed, signal=signals.spider_closed)
		return o
		
	def process_request(self, request, spider):
		random_proxy = self._get_random_proxy()
		#logger.debug("Chosen proxy is %s:%s" % (random_proxy.host, random_proxy.port))
		random_proxy.stat['requests'] += 1
		request.meta['proxy'] = "http://%s:%s" % (random_proxy.host, random_proxy.port)
		request.meta['proxy_obj'] = random_proxy
			
	def process_response(self, request, response, spider):
		if 'proxy_obj' in request.meta:
			proxy = request.meta['proxy_obj']
			proxy._runtimes.append(request.meta['download_latency'])
			if response.status is not 200:
				proxy.stat['errors'][response.status] += 1
				if response.status is 407:	# Proxy authentication required
					logger.debug("Removing proxy requring authentication %s", proxy)
					self.proxies.remove(proxy)
		return response
	
	def process_exception(self, request, exception, spider):
		if 'proxy_obj' in request.meta:
			proxy = request.meta['proxy_obj']
			proxy.stat['errors'][exception.__str__()] += 1
			if sum(proxy.stat['errors'].values()) >= self.max_errors and self.proxies.count(proxy) > 0:
				logger.debug("Removing proxy exceeding max error count %s", proxy)
				self.proxies.remove(proxy)
			random_proxy = self._get_random_proxy()
			random_proxy.stat['requests'] += 1
			#logger.debug("Chosen proxy is %s:%s" % (random_proxy.host, random_proxy.port))
			request.meta['proxy'] = "http://%s:%s" % (random_proxy.host, random_proxy.port)
			request.meta['proxy_obj'] = random_proxy
		
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
		newlist = sorted(self.proxies, key=lambda x: x.stat['requests'], reverse=False)
		return newlist[0]