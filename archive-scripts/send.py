#!/usr/bin/python3
from PikaConn import PikaConn

conn = PikaConn('hello')
conn.pub()
conn.close()
