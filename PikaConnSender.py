import pika
from config import config

class PikaConnSender:
	DEFAULT_EXCHANGE = ("","")
	
	def __init__(self, queueName = "", exchange = (), durable=True, exclusive=False, severity='', topic=''):
		creds = pika.credentials.PlainCredentials(config.USER, config.SECRET)
		self.params = pika.ConnectionParameters(config.HOST)
		self.queueName = queueName
		self.durable = durable
		self.exclusive = exclusive
		self.severity = severity
		self.topic = topic
		self.isExchange = exchange != PikaConnSender.DEFAULT_EXCHANGE
		self.exchangeName = "" if not self.isExchange else exchange[0]
		self.exchangeType = "" if not self.isExchange else exchange[1]
		if not self.isExchange:
			self.params = pika.ConnectionParameters(config.HOST, config.PORT, config.VENV, creds)
		
		self.connection = pika.BlockingConnection(self.params)
		self.channel = self.connection.channel()	
	
	def setChannel(self):
		self.channel.exchange_declare(exchange=self.exchangeName, exchange_type=self.exchangeType)
		
	def getQueueName(self):
		return self.queueName
	
	def bindQueue(self):
		self.channel.queue_bind(exchange=self.exchangeName, queue=self.queueName)
	
	def close(self):
		self.connection.close()
		print("\nConnection closed")

	def publish(self, body):
		routing_key = ""
		if self.severity != "":
			routing_key = self.severity
		if self.topic != "":
			routing_key = self.topic
		
		if self.isExchange:
			self.channel.basic_publish(
				exchange=self.exchangeName, routing_key=routing_key, body=body
			)
		else:
			if self.durable:
				self.channel.basic_publish(exchange="", routing_key=self.queueName, body=body,
                            properties=pika.BasicProperties(
								delivery_mode=pika.DeliveryMode.Persistent
							))
			else:
				self.channel.basic_publish(exchange="", routing_key=self.queueName, body=body)
		
		if routing_key == "":
			print(f" [x] Sent '{body}'")
		else:
			print(f" [X] Sent {routing_key}: '{body}'")
	
	def getChannel(self):
		return self.channel

	def getConnection(self):
		return self.connection
