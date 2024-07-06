#!/root/rabbitmq-scripts/env_pika/bin/python3
from PikaConn import PikaConn
import sys, os, time
import click

@click.command()
@click.option('--queue', '-q', default='queue name', show_default=True)
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
	return conn

def subscribe(conn):
	if conn.isExchange:
		conn.channel.exchange_declare(exchange = conn.exchangeName, exchange_type= conn.exchangeType)
		result = conn.channel.queue_declare("", exclusive=conn.exclusive)
		tempQueueName = result.method.queue
		conn.channel.queue_bind(exchange=conn.exchangeName, queue=tempQueueName)
		print(' [*] Waiting for logs. To exit press CTRL+C')

		def callback(ch, method, properties, body):
			print(f" [x] {body}")

		conn.channel.basic_consume(
			queue=tempQueueName, on_message_callback=callback, auto_ack=True)

	else:
		conn.channel.queue_declare(queue=conn.queueName, durable=conn.durable)
		print(' [*] Waiting for logs. To exit press CTRL+C')

		def callback(ch, method, properties, body):
			print(f" [x] Received {body.decode()}")
			time.sleep(body.count(b'.'))
			print(" [x] Done")
			ch.basic_ack(delivery_tag=method.delivery_tag)

		conn.channel.basic_qos(prefetch_count=1)
		conn.channel.basic_consume(queue=conn.getQueueName(), on_message_callback=callback)

try:
	conn = createConnection()
	print(conn.queueName)
	subscribe(conn)
	print("subscribed")
	conn.channel.start_consuming()
except KeyboardInterrupt:
	print('user interrupted')
	try:
		sys.exit(0)
	except SystemExit:
		os._exit(0)
	finally:
		conn.close()
