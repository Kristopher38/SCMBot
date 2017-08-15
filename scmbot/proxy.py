from proxybroker import Proxy
from collections import Counter
from datetime import datetime

class ProxyExtended(Proxy):
	def __init__(self, proxy):
		self.__dict__ = proxy.__dict__
		self.__class__ = ProxyExtended
		self.stat['status'] = Counter()
		self.last_used = datetime.min
		self.hold_until = datetime.min
		