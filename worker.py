#!/root/rabbitmq-scripts/env_pika/bin/python3
from PikaConnReceiver import PikaConnReceiver
import sys, os
import click

@click.command()
@click.option('--queue', '-q', default='', show_default=True)
@click.option('--durable', '-d', default=False, show_default=True)
@click.option('--exchange', '-e', default='', show_default=True)
@click.option('--type', '-t', default='', show_default=True)
@click.option('--exclusive', '-x', default=False, show_default=True)
@click.option('--severity', '-s', default=(), show_default=True, multiple = True)
@click.option('--topic', '-p', default=(), show_default=True, multiple = True)
def receive(queue, durable, exchange, type, exclusive, severity, topic):
    conn = PikaConnReceiver(queueName=queue, 
					exchange=(exchange, type),
					durable=durable,
	        		exclusive=exclusive,
                    severity=severity,
		    topic = topic)
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
