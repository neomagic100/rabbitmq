#!/root/rabbitmq-scripts/env_pika/bin/python3
from PikaConn import PikaConn
import sys, os, time
import click

def main(conn, queueName):
	def callback(ch, method, properties, body):
		print(f" [x] Received {body.decode()}")
		time.sleep(body.count(b'.'))
		print(" [x] Done")
		ch.basic_ack(delivery_tag = method.delivery_tag)

	conn.getChannel().basic_consume(queueName, callback)

	print('waiting for messages on {}'.format(queueName if queueName.strip() != "" \
		else "temporary queue"))
	conn.getChannel().start_consuming()

@click.command()
@click.option('--queue', '-q', default='queue name', show_default=True)
@click.option('--exchange', '-e', default='', show_default=True)
@click.option('--type', '-t', default='', show_default=True)
@click.option('--exclusive', '-x', default=False, show_default=True)
@click.option('--durable', '-d', default=False, show_default=True)
def createConnection(queue = "", exchange = "", type = "", exclusive = False,
	durable = False):
	exchangeTuple=(exchange, type)
	conn = PikaConn(queueName = queue, 
					exchange = exchangeTuple, 
					durable = durable,
					exclusive = exclusive,
					sending = False)
	return conn


try:
	conn = createConnection()
	main(conn, conn.queueName)
except KeyboardInterrupt:
	print('user interrupted')
	try:
		sys.exit(0)
	except SystemExit:
		os._exit(0)
	finally:
		if conn:
			conn.close()
