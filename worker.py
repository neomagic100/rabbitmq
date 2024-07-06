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

receive()
# connection = pika.BlockingConnection(
#     pika.ConnectionParameters(host='localhost'))
# channel = connection.channel()

# channel.exchange_declare(exchange='logs', exchange_type='fanout')

# result = channel.queue_declare(queue='', exclusive=True)
# queue_name = result.method.queue

# channel.queue_bind(exchange='logs', queue=queue_name)

# print(' [*] Waiting for logs. To exit press CTRL+C')

# def callback(ch, method, properties, body):
#     print(f" [x] {body}")

# channel.basic_consume(
#     queue=queue_name, on_message_callback=callback, auto_ack=True)

# channel.start_consuming()
