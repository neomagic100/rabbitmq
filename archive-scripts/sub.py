#!/usr/bin/python3
from PikaConn import PikaConn
import sys, os

def main(conn):

	def callback(ch, method, properties, body):
		print("receive {}".format(body))

	conn.getChannel().basic_consume(queue='hello', auto_ack=True, on_message_callback=callback)

	print('waiting for messages')
	conn.getChannel().start_consuming()

if __name__ == '__main__':
	try:
		conn = PikaConn('hello')
		main(conn)
	except KeyboardInterrupt:
		print('user interrupted')
		try:
			sys.exit(0)
		except SystemExit:
			os._exit(0)
		finally:
			if conn:
				conn.close()
