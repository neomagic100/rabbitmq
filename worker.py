#!/root/rabbitmq-scripts/env_pika/bin/python3
from PikaConn import PikaConn
import sys, os, time
import click

@click.command()
@click.option('--queue', '-q', default='queue name', show_default=True)
@click.option('--exchange', '-e', default='', show_default=True)
@click.option('--type', '-t', default='', show_default=True)
@click.option('--exclusive', '-x', default=False, show_default=True)
@click.option('--durable', '-d', default=False, show_default=True)
def createConnection(queue, exchange, type, exclusive, durable):
	exchangeTuple=(exchange, type)
	conn = PikaConn(queueName = queue, 
					exchange = exchangeTuple, 
					durable = durable,
					exclusive = exclusive,
					sending = False)
	return conn

if __name__ == "__main__":
	try:
		conn = createConnection()
		conn.subscribe()
	except KeyboardInterrupt:
		print('user interrupted')
		try:
			sys.exit(0)
		except SystemExit:
			os._exit(0)
		finally:
			conn.close()
