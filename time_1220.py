# -*- coding: utf-8 -*-
# time_measurement_1220.py

from socket import *
import time
import sys
#import pbl2018

assist_port = 52699

def SIZE(client, message):
	#ファイル全体を要求し、ファイルデータを受信
	client.send(message.encode())
	size = client.recv(1024).decode()
	return size 	

def ASSIST_SIZE(connection):
	#サーバのIPアドレスあるいはホスト名、ポート番号を受信
	recv_bytearray = bytearray()
	while True:
		b = connection.recv(1)
		recv_bytearray.append(b[0]) 
		if b == b'\n':
			recv_str = recv_bytearray.decode()
			break
	recv_server_inf = recv_str.split()
	
	#クライアントが送ったSIZEメッセージを受信
	recv_me = connection.recv(1024).decode()
	return recv_server_inf[0], int(recv_server_inf[1]), recv_me
	

if __name__ == '__main__':
	# main program
	assist_socket = socket(AF_INET, SOCK_STREAM)
	assist_socket.bind(('', assist_port))
	assist_socket.listen(1)
	print('The assist is ready to receive')

	while True:
		# クライアントからの接続があったら、それを受け付け、
		# そのクライアントとの通信のためのソケットを作る
		s, addr = assist_socket.accept()
		message = s.recv(1024).decode().split()
		send_data = ''
		if message[0] == 'direct':   
			for i in range(1500):
				send_data += 'a' 		
			s.send(("{}\n".format(send_data)).encode())
			print('send {}'.format(sys.getsizeof(send_data)))
		elif message[0] == 'assist':
			server_name, server_port = message[1], int(message[2])

			#サーバ -> 経由地
			s_socket = socket(AF_INET, SOCK_STREAM)
			s_socket.connect((server_name, server_port))	
			s_socket.send(('direct').encode())
			recv_bytearray = bytearray()
			while True:
				b = s_socket.recv(1)
				recv_bytearray.append(b[0]) 
				if b == b'\n':
					recv_str = recv_bytearray.decode()
					break
			data = recv_str
			s_socket.close()

			#経由地 -> クライアント
			s.send(data.encode())

