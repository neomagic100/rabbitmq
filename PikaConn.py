import pika
from config import config

class PikaConn:
	staticQueueName = None

	def __init__(self, queueName = "", exchange = (), durable=False, exclusive=False, sending=False):
		creds = pika.credentials.PlainCredentials(config.USER, config.SECRET)
		if exchange == ():
			params = pika.ConnectionParameters(config.HOST, config.PORT, config.VENV, creds)
		else:
			params = pika.ConnectionParameters(config.HOST)
		self.connection = pika.BlockingConnection(params)
		self.channel = self.connection.channel()
		self.exchangeName = "" if exchange == () else exchange[0]
		self.exchangeType = "" if exchange == () else exchange[1]
		if exchange == ():
			self.channel.queue_declare(queue=queueName, durable=durable, exclusive=exclusive)
		else:
			self.setChannel(self.exchangeName, self.exchangeType)
		if not sending:
			if self.exchangeName != "":
				self.setChannel(self.exchangeName, self.exchangeType)
				result = self.channel.queue_declare(queue=queueName, durable=durable, exclusive=exclusive)
				PikaConn.staticQueueName = result.method.queue
				self.staticQ = PikaConn.staticQueueName
				self.channel.queue_bind(exchange=self.exchangeName, queue=self.staticQ)
	
	def setChannel(self, exchange='', exchange_type=''):
		if exchange == '' and exchange_type == '':
			return
		self.channel.exchange_declare(exchange=exchange, exchange_type=exchange_type)
		

	def fetchQueueName(self):
		return self.staticQ
	
	def bindQueue(self):
		self.channel.queue_bind(exchange=self.exchangeName, queue=self.fetchQueueName())
	
	def close(self):
		self.connection.close()
		print("connection closed")

	def pub(self, exchange, routing_key, body, persist = False, useExchange = None):
		exchangeType = None
		if useExchange and isinstance(useExchange, tuple):
			self.setChannel(useExchange[0], useExchange[1])
			exchangeType = 'tuple'
		elif useExchange and isinstance(useExchange, dict):
			self.setChannel(useExchange['exchange'], useExchange['exchange_type'])
			exchangeType = 'dict'
		if persist:
			self.channel.basic_publish(exchange=exchange,
						   routing_key=routing_key,
						   body=body,
						   properties=pika.BasicProperties(
							delivery_mode = pika.DeliveryMode.Persistent))

		else:
			self.channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body)

		print("sent '{}' on {} {}".format(body, routing_key if routing_key.strip() != '' else 'temporary queue', 'persist' if persist else ''),
			end=' ')
		if useExchange:
			if not exchangeType:
				return
			if exchangeType == 'dict':
				exch = useExchange['exchange']
				exchT = useExchange['exchange_type']
			elif exchangeType == 'tuple':
				exch, exchT = useExchange
			print("using exchange {} of type {}".format(exch, exchT))

		print()		

	def getChannel(self):
		return self.channel

	def getConnection(self):
		return self.connection
	
	@staticmethod
	def getQueueName():
		return PikaConn.staticQueueName

	def getQueueName(self):
		return self.staticQ
