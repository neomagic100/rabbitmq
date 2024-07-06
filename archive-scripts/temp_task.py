#!/root/rabbitmq-scripts/env_pika/bin/python3
from PikaConn import PikaConn
from config import config
import sys
import click

@click.command()
@click.option('--message', '-m', default='insert message here', show_default=True)
def send(message):
	exchangeName="logs"
	exchangeType="fanout"
	conn = PikaConn('', exchange=(exchangeName, exchangeType),  exclusive=True, sending=True)
	message = message
	conn.pub(exchangeName, '', message, useExchange=(exchangeName, exchangeType))
	print(f" [x] Sent {message}")

send()

