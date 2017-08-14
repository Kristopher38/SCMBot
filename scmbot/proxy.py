from proxybroker import Proxy
from collections import Counter
import copy

class ProxyExtended(Proxy):
	def __init__(self, proxy):
		#self = copy.copy(proxy)
		self.__dict__ = proxy.__dict__
		self.__class__ = ProxyExtended
		self.stat['status'] = Counter()
		