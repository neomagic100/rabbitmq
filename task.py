#!/root/rabbitmq-scripts/env_pika/bin/python3
from PikaConnSender import PikaConnSender
import click

@click.command()
@click.option('--message', '-m', default='insert message here', show_default=True)
@click.option('--queue', '-q', default='', show_default=True)
@click.option('--durable', '-d', default=False, show_default=True)
@click.option('--exchange', '-e', default='', show_default=True)
@click.option('--type', '-t', default='', show_default=True)
@click.option('--exclusive', '-x', default=False, show_default=True)
@click.option('--severity', '-s', default='', show_default=True)
@click.option('--topic', '-p', default='', show_default=True)
def send(message, queue, durable, exchange, type, exclusive, severity, topic):
	conn = PikaConnSender(queueName=queue, 
					exchange=(exchange, type),
					durable=durable,
					exclusive=exclusive,
     				severity=severity,
				topic=topic)
	conn.publish(message)
	return conn

conn = send()
conn.close()
