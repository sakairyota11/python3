# -*- coding: utf-8 -*-
# client_assist.py

from socket import *
from multiprocessing.pool import ThreadPool	#スレッド使用のため
import time
import sys
# import pbl2018

assist_port = 52601

def GET(client, fs, gm):
	#ファイル全体を要求し、ファイルデータを受信
	message = gm
	print(message)
	client.send(message.encode())
	recv_bytearray = bytearray()
	while True:
		b = client.recv(1)
		recv_bytearray.append(b[0]) 
		if b == b'\n':
			recv_str = recv_bytearray.decode()
			break
	recv_message = recv_str.split()
	print(recv_str)
	if recv_message[0] == 'OK':				
#		data = client.recv(fs).decode()
		BUFSIZE = fs
		data = ''
		while True:	 	
			data += client.recv(BUFSIZE).decode()
			if len(data) >= BUFSIZE:
				break
		return data	
	elif recv_message[1] == '101':
		print('No such file.\n')
		sys.exit()
	elif recv_message[1] == '102':
		print('Invalid range.\n')
		sys.exit()
	else:
		print('Please rewrite the command.\n')
		sys.exit()	

#並列処理で行う関数
def THREADING(sn, sp, fs, gm):	#引数　サーバの名前、サーバポート、ファイルサイズ、GET要求メッセージ
	thread_socket = socket(AF_INET, SOCK_STREAM)
	thread_socket.connect((sn, sp))
	data = GET(thread_socket, fs, gm)
	thread_socket.close()	
	return data


def GET_PARTIAL(fs, gm, sn, sp):
	#ファイルを要求し、ファイルデータを受信
	message = gm.split()
	#assistとserverのやりとりをスレッドで行う
	partial_num = 3		#分割数
	thread_num = partial_num
	head = [None]*partial_num
	tail = [None]*partial_num
	new_message = [None]*partial_num
	pool = [None]*thread_num
	threads = [None]*thread_num
	data = [None]*thread_num
	size = [None]*partial_num
	file_data = ''
	#clientからの要求がALLのとき
	if message[3] == 'ALL':
		for i in range(partial_num):
			head[i] = fs//partial_num * i
			tail[i] = fs//partial_num * (i + 1)
			if i == partial_num - 1:
				tail[i] = fs
			new_message[i] = 'GET {} {} PARTIAL {} {}\n'.format(message[1], message[2], head[i], tail[i])
			size[i] = int(tail[i]) - int(head[i])
		for i in range(thread_num):
			pool[i] = ThreadPool(processes=1)
		for i in range(thread_num):
			threads[i] = pool[i].apply_async(THREADING, args=(server_name, server_port, size[i], new_message[i]))
		for i in range(thread_num):
			data[i] = threads[i].get()
		for i in range(len(data)):
			file_data += data[i]
	#clientからの要求がPARTIALのとき
	elif message[3] == 'PARTIAL':
		size_sum = int(message[5]) - int(message[4])
		for i in range(partial_num):
			head[i] = int(message[4]) + size_sum//partial_num * i
			tail[i] = int(message[4]) + size_sum//partial_num * (i + 1)
			if i == partial_num - 1:
				tail[i] = message[5]
			size[i] = int(tail[i]) - int(head[i])
			new_message[i] = 'GET {} {} PARTIAL {} {}\n'.format(message[1], message[2], head[i], tail[i]) 			
		for i in range(thread_num):
			pool[i] = ThreadPool(processes=1)
		for i in range(thread_num):
			threads[i] = pool[i].apply_async(THREADING, args=(server_name, server_port, size[i], new_message[i]))
		for i in range(thread_num):
			data[i] = threads[i].get()
		for i in range(len(data)):
			file_data += data[i]	
	return file_data

def ASSIST(connection):
	#clientから送信されたserverのIPアドレスあるいはホスト名、ポート番号、GET要求、ファイルサイズを返す
	#サーバのIPアドレスあるいはホスト名、ポート番号を受信
	recv_bytearray = bytearray()
	while True:
		b = connection.recv(1)
		recv_bytearray.append(b[0]) 
		if b == b'\n':
			recv_str = recv_bytearray.decode()
			break
	#クライアントが送ったGETメッセージを受信
	recv_server_inf = recv_str.split()
	recv_bytearray2 = bytearray()
	while True:
		b = connection.recv(1)
		recv_bytearray2.append(b[0]) 
		if b == b'\n':
			recv_get = recv_bytearray2.decode()
			break
	recv_bytearray3 = bytearray()
	#ファイルサイズを受信
	size = connection.recv(1024).decode()
	return recv_server_inf[0], int(recv_server_inf[1]), recv_get, int(size)
	

if __name__ == '__main__':
	# main program
	assist_socket = socket(AF_INET, SOCK_STREAM)
	assist_socket.bind(('', assist_port))
	assist_socket.listen(1)
	print('The assist is ready to receive')

	while True:
		# clientからの接続があったら、それを受け付け、
		# そのclientとの通信のためのソケットを作る
		connection_socket, addr = assist_socket.accept()

		#client -> 経由地 -> server
		#clientから送られてきたGET要求をserverに送る
		server_name, server_port, get_message, file_size = ASSIST(connection_socket)
		
		#server-> 経由地
		#serverに送信したGET要求に対して送られてきたファイルデータを受信
		get_socket = socket(AF_INET, SOCK_STREAM)
		get_socket.connect((server_name, server_port))	
		file_data = GET(get_socket, file_size, get_message)
		get_socket.close()
		print(len(file_data))

		#経由地 -> client
		#ファイルデータをclientに送信
		connection_socket.send(file_data.encode())

