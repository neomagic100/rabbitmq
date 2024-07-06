#!/root/rabbitmq-scripts/env_pika/bin/python3
from PikaConn import PikaConn
from config import config
import sys
import click

@click.command()
@click.option('--message', '-m', default='insert message here', show_default=True)
@click.option('--queue', '-q', default='queue name', show_default=True)
def send(message, queue):
	conn = PikaConn(queue)
	message = message or "Hello workers!"
	conn.pub('', queue, message, persist=True)
	print(f" [x] Sent {message}")
	with open("pika-queue.dat", "w") as f:
		f.write(queue)


send()

