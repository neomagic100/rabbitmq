#!/root/rabbitmq-scripts/env_pika/bin/python3
import pika
from PikaConn import PikaConn
import sys, os, time
import click

@click.command()
@click.option('--queue', '-q', default='', show_default=True)
@click.option('--exchange', '-e', default='', show_default=True)
@click.option('--type', '-t', default='', show_default=True)
@click.option('--exclusive', '-x', default=True, show_default=True)
@click.option('--durable', '-d', default=False, show_default=True)
def createConnection(queue, exchange, type, exclusive, durable):
	exchangeTuple=(exchange, type)
	conn = PikaConn(queueName = queue, 
					exchange = exchangeTuple, 
					durable = durable,
					exclusive = exclusive,
					sending = False)
#	print(f"{conn.queueName} '{conn.isExchange}'")
	return conn

def subscribe(pc):
	connection = pika.BlockingConnection(pc.params)
	channel = connection.channel()
	print(conn.isExchange)
	if conn.isExchange:
		channel.exchange_declare(exchange = conn.exchangeName, exchange_type= conn.exchangeType)
		result = channel.queue_declare("", exclusive=conn.exclusive)
		tempQueueName = result.method.queue
		channel.queue_bind(exchange=conn.exchangeName, queue=tempQueueName)
		print(' [*] Waiting for logs. To exit press CTRL+C')

		def callback(ch, method, properties, body):
			print(f" [x] {body}")

		channel.basic_consume(
			queue=tempQueueName, on_message_callback=callback, auto_ack=True)

	else:
		channel.queue_declare(queue=conn.queueName, durable=conn.durable)
		print(' [*] Waiting for logs. To exit press CTRL+C')

		def callback(ch, method, properties, body):
			print(f" [x] Received {body.decode()}")
			time.sleep(body.count(b'.'))
			print(" [x] Done")
			ch.basic_ack(delivery_tag=method.delivery_tag)

		channel.basic_qos(prefetch_count=1)
		channel.basic_consume(queue=conn.getQueueName(), on_message_callback=callback)
  
	return channel

try:
	print('trying')
	conn = createConnection()
	print("!!!!!!!!!!!!!!!---------------!!!!!!!!!!!!!!!!!!!!")
	channel = subscribe(conn)
	print("subscribed")
	channel.start_consuming()
except KeyboardInterrupt:
	print('user interrupted')
	try:
		print("sys.exit")
		sys.exit(0)
	except SystemExit:
		print("SystemExit")
		os._exit(0)
	finally:
		conn.close()
