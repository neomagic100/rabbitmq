#!/root/rabbitmq-scripts/env_pika/bin/python3
#import pika
from PikaConn import PikaConnReceiver
import sys, os, time
import click

import pika

@click.command()
@click.option('--queue', '-q', default='', show_default=True)
@click.option('--durable', '-d', default=False, show_default=True)
@click.option('--exchange', '-e', default='', show_default=True)
@click.option('--type', '-t', default='', show_default=True)
@click.option('--exclusive', '-x', default=False, show_default=True)
def receive(queue, durable, exchange, type, exclusive):
    conn = PikaConnReceiver(queueName=queue, 
					exchange=(exchange, type),
					durable=durable,
	        		exclusive=exclusive)
    conn.consume()
    return conn

connection = None

try:
    connection = receive()
except KeyboardInterrupt:
	print('user interrupted')
	try:
		print("sys.exit")
		sys.exit(0)
	except SystemExit:
		print("SystemExit")
		os._exit(0)
	finally:
		connection.close()
		print("Connection closed")