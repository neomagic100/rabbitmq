import pika
import time
import asyncio
from config import config

class PikaConnSender:
	DEFAULT_EXCHANGE = ("","")
	
	def __init__(self, queueName = "", exchange = (), durable=True, exclusive=False, severity=''):
		creds = pika.credentials.PlainCredentials(config.USER, config.SECRET)
		self.params = pika.ConnectionParameters(config.HOST)
		self.queueName = queueName
		self.durable = durable
		self.exclusive = exclusive
		self.severity = severity
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
		if self.isExchange:
			self.channel.basic_publish(
				exchange=self.exchangeName, routing_key=self.severity, body=body
			)
		else:
			if self.durable:
				self.channel.basic_publish(exchange="", routing_key=self.queueName, body=body,
                            properties=pika.BasicProperties(
								delivery_mode=pika.DeliveryMode.Persistent
							))
			else:
				self.channel.basic_publish(exchange="", routing_key=self.queueName, body=body)

		print (f" [x] Sent '{body}'")
	
	def getChannel(self):
		return self.channel

	def getConnection(self):
		return self.connection

class PikaConnReceiver:
	def __init__(self, queueName = "", exchange = (), durable=True, exclusive=False, severity=[]):
		creds = pika.credentials.PlainCredentials(config.USER, config.SECRET)
		self.params = pika.ConnectionParameters(config.HOST)
		self.queueName = queueName
		self.durable = durable
		self.exclusive = exclusive
		self.severity = severity
		self.isExchange = exchange != PikaConnSender.DEFAULT_EXCHANGE
		self.exchangeName = "" if not self.isExchange else exchange[0]
		self.exchangeType = "" if not self.isExchange else exchange[1]
		if not self.isExchange:
			self.params = pika.ConnectionParameters(config.HOST, config.PORT, config.VENV, creds)
		
	def consume(self):
		if self.isExchange:
			self.consumeExchange()
		else:
			self.consumeQueue()

	def consumeExchange(self):
		connection = pika.BlockingConnection(
    		pika.ConnectionParameters(host=config.HOST))
		channel = connection.channel()

		channel.exchange_declare(exchange=self.exchangeName, exchange_type=self.exchangeType)

		result = channel.queue_declare(queue='', exclusive=True)
		queue_name = result.method.queue
		callbackFn = None
		if self.severity == []:
			channel.queue_bind(exchange=self.exchangeName, queue=queue_name)
			def callback(ch, method, properties, body):
				print(f" [x] {body}")
			callbackFn = callback
		else:
			for sev in self.severity:
				channel.queue_bind(exchange=self.exchangeName, queue=queue_name, routing_key=sev)
			def callback(ch, method, properties, body):
				print(f" [x] {method.routing_key}:{body}")
			callbackFn = callback

		print(' [*] Waiting for logs. To exit press CTRL+C')

		channel.basic_consume(
			queue=queue_name, on_message_callback=callbackFn, auto_ack=True)

		channel.start_consuming()

	def consumeQueue(self):
		connection = pika.BlockingConnection(
    		pika.ConnectionParameters(host=config.HOST))
		channel = connection.channel()
		channel.queue_declare(queue=self.queueName, durable=True)
		print(' [*] Waiting for messages. To exit press CTRL+C')


		def callback(ch, method, properties, body):
			print(f" [x] Received {body.decode()}")
			time.sleep(body.count(b'.'))
			ch.basic_ack(delivery_tag=method.delivery_tag)

		channel.basic_qos(prefetch_count=1)
		channel.basic_consume(queue=self.queueName, on_message_callback=callback)
		channel.start_consuming()
