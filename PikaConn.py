import pika
import time
import asyncio
from config import config

class PikaConn:
	DEFAULT_EXCHANGE = ("","")
	
	def __init__(self, queueName = "", exchange = (), durable=True, exclusive=False, sending=False):
		creds = pika.credentials.PlainCredentials(config.USER, config.SECRET)
		params = pika.ConnectionParameters(config.HOST)
		self.queueName = queueName
		self.durable = durable
		self.exclusive = exclusive
		self.isExchange = exchange != PikaConn.DEFAULT_EXCHANGE
		self.exchangeName = "" if not self.isExchange else exchange[0]
		self.exchangeType = "" if not self.isExchange else exchange[1]
		# if not self.isExchange else \
			#pika.ConnectionParameters(config.HOST, config.PORT, config.VENV, creds)

		self.connection = pika.BlockingConnection(params)
		self.channel = self.connection.channel()	
		self.activeQueue = None
  
		if self.isExchange:
			self.channel.exchange_declare(exchange = self.exchangeName, exchange_type= self.exchangeType)
		elif not self.isExchange and self.queueName != "":
			self.channel.queue_declare(queue=self.queueName, durable=durable)
	
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
				exchange=self.exchangeName, routing_key="", body=body
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
  
	async def subscribe(self):
		if self.isExchange:
			tempQueueName = self.activeQueue.method.queue
			self.channel.queue_bind(exchange=self.exchangeName, queue=tempQueueName)
			print(' [*] Waiting for logs. To exit press CTRL+C')

			def callback(ch, method, properties, body):
				print(f" [x] {body}")
    
			self.channel.basic_consume(
				queue=tempQueueName, on_message_callback=callback, auto_ack=True)

		else:
			print(' [*] Waiting for logs. To exit press CTRL+C')

			def callback(ch, method, properties, body):
				print(f" [x] Received {body.decode()}")
				time.sleep(body.count(b'.'))
				print(" [x] Done")
				ch.basic_ack(delivery_tag=method.delivery_tag)
    
			self.channel.basic_qos(prefetch_count=1)
			self.channel.basic_consume(queue=self.queueName, on_message_callback=callback)
		
	#	self.channel.start_consuming()
		asyncio.run(self.consume())
	
	async def consume(self):
		await self.channel.start_consuming()
	def getChannel(self):
		return self.channel

	def getConnection(self):
		return self.connection
