#!/root/rabbitmq-scripts/env_pika/bin/python3
from PikaConn import PikaConn
import sys, os, time

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

if __name__ == '__main__':
	try:
		queueName = ''
		try:
			with open('pika-queue.dat', 'r') as f:
				text = f.readline()
				queueName = str(text)
				print("reading from queue {}".format(queueName))
		except:
			print("Using temporary queue")
		conn = PikaConn( exchange=('logs', 'fanout'), exclusive=True)
		main(conn, queueName)
	except KeyboardInterrupt:
		print('user interrupted')
		try:
			sys.exit(0)
		except SystemExit:
			os._exit(0)
		finally:
			if conn:
				conn.close()
