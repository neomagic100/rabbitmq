import time
import pika
from config import config

class PikaConnReceiver:
	DEFAULT_EXCHANGE = ("","")
    
	def __init__(self, queueName = "", exchange = (), durable=True, exclusive=False, severity=(), topic=()):
		creds = pika.credentials.PlainCredentials(config.USER, config.SECRET)
		self.params = pika.ConnectionParameters(config.HOST)
		self.queueName = queueName
		self.durable = durable
		self.exclusive = exclusive
		self.severity = severity
		self.topic = topic
		self.isExchange = exchange != PikaConnReceiver.DEFAULT_EXCHANGE
		self.exchangeName = "" if not self.isExchange else exchange[0]
		self.exchangeType = "" if not self.isExchange else exchange[1]
		if not self.isExchange:
			self.params = pika.ConnectionParameters(config.HOST, config.PORT, config.VENV, creds)

	def isSeverity(self):
		if isinstance(self.severity, tuple):
			return self.severity != ()
		if isinstance(self.severity, str):
			return self.severity != ""
		return False # safety

	def isTopic(self):
		if isinstance(self.topic, tuple):
			return self.topic != ()
		if isinstance(self.topic, str):
			return self.topic != ""
		return False # safety
		
	def consume(self):
		if (self.isExchange and self.isSeverity()) or \
			(self.isExchange and self.isTopic()):
			self.consumeExchangeRoutingId()
		elif self.isExchange:
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
		channel.queue_bind(exchange=self.exchangeName, queue=queue_name)
		
		def callback(ch, method, properties, body):
			print(f" [x] {body}")
		print(' [*] Waiting for logs. To exit press CTRL+C')

		channel.basic_consume(
			queue=queue_name, on_message_callback=callback, auto_ack=True)

		channel.start_consuming()
	
	def consumeExchangeRoutingId(self):
		connection = pika.BlockingConnection(
	    		pika.ConnectionParameters(host=config.HOST))
		channel = connection.channel()

		channel.exchange_declare(exchange=self.exchangeName, exchange_type=self.exchangeType)

		result = channel.queue_declare(queue='', exclusive=True)
		queue_name = result.method.queue
		
		if self.isSeverity():
			bindingKeys = self.severity
		else:
			bindingKeys = self.topic
   
		for key in bindingKeys:
			channel.queue_bind(exchange=self.exchangeName, queue=queue_name, routing_key=key)
		def callback(ch, method, properties, body):
			print(f" [x] {method.routing_key}:{body}")

		print(' [*] Waiting for logs. To exit press CTRL+C')

		channel.basic_consume(
			queue=queue_name, on_message_callback=callback, auto_ack=True)

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
  