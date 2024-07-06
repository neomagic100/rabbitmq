import pika
from config import config

class PikaConn:
	
	def __init__(self, queueName = "", exchange = (), durable=False, exclusive=False, sending=False):
		creds = pika.credentials.PlainCredentials(config.USER, config.SECRET)
		self.queueName = queueName
		self.isExchange = exchange != ()
		self.durable = durable
		self.exclusive = exclusive
		params = pika.ConnectionParameters(config.HOST) if not self.isExchange else \
			pika.ConnectionParameters(config.HOST, config.PORT, config.VENV, creds)

		self._makeConnection(params, queueName, exchange)
		if sending and self.isExchange:
			self._createSenderExchange()
		elif sending and not self.isExchange:
			self._createSenderQueue()
		elif not sending and self.isExchange:
			self._createReceiverExchange()
		elif not sending and not self.isExchange:
			self._createReceiverQueue()

	def _makeConnection(self, params, queue, exchange):
		self.connection = pika.BlockingConnection(params)
		self.channel = self.connection.channel()
		self.queueName = queue
		self.exchangeName = "" if not self.isExchange else exchange[0]
		self.exchangeType = "" if not self.isExchange else exchange[1]

	def _createSenderQueue(self):
		self.channel.queue_declare(queue=self.queueName, 
									durable=self.durable, 
									exclusive=self.exclusive)

	def _createReceiverQueue(self):
		self._createSenderQueue()

	def _createSenderExchange(self):
		self.setChannel()

	def _createReceiverExchange(self):
		self._createSenderExchange()
		self.setChannel()
		result = self.channel.queue_declare(queue=self.queueName,
											durable=self.durable,
											exclusive=self.exclusive)
		self.queueName = result.method.queue
		self.channel.queue_bind(exchange=self.exchangeName, queue=self.queueName)
	
	def setChannel(self):
		self.channel.exchange_declare(exchange=self.exchangeName, exchange_type=self.exchangeType)
		
	def getQueueName(self):
		return self.queueName
	
	def bindQueue(self):
		self.channel.queue_bind(exchange=self.exchangeName, queue=self.queueName)
	
	def close(self):
		self.connection.close()
		print("\nConnection closed")

	def publish(self, body, exchange = None, routing_key = None, persist = False):
		if persist:
			self.channel.basic_publish(exchange=exchange if exchange else self.exchangeName,
						   routing_key=routing_key if routing_key else self.queueName,
						   body=body,
						   properties=pika.BasicProperties(
							delivery_mode = pika.DeliveryMode.Persistent))
		else:
			self.channel.basic_publish(exchange=exchange if exchange else self.exchangeName, 
										routing_key=routing_key if routing_key else self.queueName, 
										body=body)

		print("sent '{}' on {} {}".format(body, routing_key if routing_key.strip() != '' \
			 else 'temporary queue', 'persist' if persist else ''), end=' ')
		if self.isExchange:
			print("using exchange {} of type {}".format(self.exchangeName, self.exchangeType))

		print()		

	def getChannel(self):
		return self.channel

	def getConnection(self):
		return self.connection
