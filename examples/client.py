import socket
import sys

host="localhost"

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

port  = int(sys.argv[1])

while True:
	inp = raw_input()
	sock.sendto(inp, (host, port))
	data,addr = sock.recvfrom(1024)
	print data