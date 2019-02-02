# -*- coding: utf-8 -*-
# client_1220.py
#https://stackoverflow.com/questions/6893968/how-to-get-the-return-value-from-a-thread-in-python

from socket import *
from multiprocessing.pool import ThreadPool	#スレッド使用のため	
import time
import sys
import pbl2018
import threading	#スレッド使用のため

def SIZE(client, fn):
	#要求するファイルのデータサイズを取得
	client.send(('SIZE ' + fn + '\n').encode())	#ファイルのサイズを要求
	message = client.recv(1024).decode()		#メッセージを受け取る
	print(message)
	message = message.split()

	if message[0] == 'OK':
		return int(message[2])
	elif message[1] == 101 :
		print('Please change the file name.\n')
		sys.exit()
	else :
		print('Please rewrite the command.\n') 
		sys.exit()

def GET(client, fs, fn, gd):
	#ファイル全体を要求し、ファイルデータを受信
	message = 'GET ' + fn + ' ' + gd + ' ALL\n'
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
#		data = client.recv(int(fs)).decode()
		BUFSIZE = int(fs)
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

def GET_PARTIAL(client, sn, sp, fn, gd, head, tail):
	#ファイルの一部を要求
	message = 'GET ' + fn + ' ' + gd + ' PARTIAL ' + str(head) + ' ' + str(tail) + '\n'
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
	if recv_message[0] == 'OK':				
#		data = client.recv(tail - head).decode()
		BUFSIZE = tail - head
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
	return data


def FILE_MAKE(data, fn):
	#ファイル作成
	f = open(fn, 'w')
	f.write(data)
	f.close()

def FILE_ADD(data, fn):
	#ファイルに追記
	f = open(fn, 'a')
	f.write(data)
	f.close()
	
def REP(client, fn, dig):
	#受信したファイルのダイジェストをサーバに報告
	client.send(('REP ' + fn + ' ' + dig + '\n').encode())
	message_recv = client.recv(1024).decode()
	message = message_recv.split()
	print(message_recv)
	if message[0] == 'OK':
		print('Tile transfer finished!') 
	elif message[1] == 101 :
		print('Please change the file name.\n')
		sys.exit()
	elif message[1] =='103':
		print('Failed to receive file data.\n')
		sys.exit()
	else :
		print('Please rewrite the command.\n') 
		sys.exit()

def ASSIST(client, sn, sp, fn, fs, gd):
	#経由地にサーバのIPアドレスあるいはホスト名、ポート番号、GETメッセージ、ファイルサイズを送信
	#ファイル全体を要求
	message = str(sn) + ' ' + str(sp) +  '\n'
	get = 'GET ' + fn + ' ' + gd + ' ALL\n'
	client.send((message+ ' ' + get + str(fs)).encode())
	#経由地からデータを受信
#	data = client.recv(fs).decode()
	BUFSIZE = fs
	data = ''
	while True:	 	
		data += client.recv(BUFSIZE).decode()
		if len(data) >= BUFSIZE:
			break
	return data

def ASSIST_SIZE(client, sn, sp, fn):
	server_inf = str(sn) + ' ' + str(sp) +  '\n'
	message = 'SIZE ' + fn + '\n'
	client.send((server_inf + '' + message).encode())
	size_message = client.recv(1024).decode()
	return size_message

def ASSIST_PARTIAL(client, sn, sp, fn, gd, head, tail):
	#経由地を通してファイルの一部を要求
	message = str(sn) + ' ' + str(sp) +  '\n'
	get = 'GET ' + fn + ' ' + gd + ' PARTIAL ' + str(head) + ' ' + str(tail) + '\n'
	print(get)
	client.send((message + ' ' + get + str(tail - head)).encode())
	BUFSIZE = int(tail - head)
	data = ''
	while True:	 	
		data += client.recv(BUFSIZE).decode()
		if len(data) >= BUFSIZE:
			break
	return data

def THREADING(sn, sp, fn, gd, head, tail):
	#サーバから直接
	#スレッドを使用し、ファイルを分割して要求
	thread_name = sn	
	thread_port = sp		
	thread_socket = socket(AF_INET, SOCK_STREAM)
	thread_socket.connect((thread_name, thread_port))
	data = GET_PARTIAL(thread_socket, sn, sp, fn, gd, head, tail)
	thread_socket.close()	
	return data

def THREADING_ASSIST(an, ap, sn, sp, fn, gd, head, tail):
	#assist経由
	#スレッドを使用し、ファイルを分割して要求
#	time.sleep(3.0)
	thread_name = an	
	thread_port = ap		
	thread_socket = socket(AF_INET, SOCK_STREAM)
	thread_socket.connect((thread_name, thread_port))
	data = ASSIST_PARTIAL(thread_socket, sn, sp, fn, gd, head, tail)
	thread_socket.close()
	return data

def bandwidth_measurement(server_name):
	bandwidth = {}
	recv_bytearray = [None]*6
	#direct
#	get_socket = socket(AF_INET, SOCK_STREAM)
#	get_socket.connect((server_name, 52699))
#	start_time = time.time()
#	get_socket.send(('direct').encode())
#	recv_bytearray[0] = bytearray()
#	while True:
#		b = get_socket.recv(1)
#		recv_bytearray[0].append(b[0]) 
#		if b == b'\n':
#			recv_str = recv_bytearray[0].decode()
#			break
#	data = recv_str
#	get_socket.close()
#	end_time = time.time()	
#	direct_server_time = end_time - start_time
#	print('direct_server_time = {} [sec]'.format(direct_server_time))
#	print('direct_bandwidth   = {} [bps]'.format(sys.getsizeof(data)*8/direct_server_time))
#	print('sys.getsizeof(data) = {}'.format(sys.getsizeof(data)))
#	bandwidth["direct"] = sys.getsizeof(data)*8/direct_server_time
	
	#assist
	assist_name = ['pbl1', 'pbl2', 'pbl3', 'pbl4', 'pbl5', 'pbl6', 'pbl7']	#ホスト名
	assist_name.remove(server_name)
	assist_name.remove('pbl4')	#注:clientになるホスト名を除く 引数でclientになるホスト名を与える？
	for i in range(len(assist_name)):	
		assist_socket = socket(AF_INET, SOCK_STREAM)
		assist_socket.connect((assist_name[i], 52691))
		start_time = time.time()
		assist_socket.send(('assist {} {}'.format(server_name, 52691)).encode())
		recv_bytearray[i] = bytearray()
		recv_str = 0
		while True:	 	
			b = assist_socket.recv(1)
			recv_bytearray[i].append(b[0])
			if b == b'\n':
				recv_str = recv_bytearray[i].decode()
				break
		data = recv_str
		assist_socket.close()
		end_time = time.time()	
		pbl_time = end_time - start_time
		print('{}_time          = {} [sec]'.format(assist_name[i], pbl_time))
		print('{}_bandwidth     = {} [bps]'.format(assist_name[i], sys.getsizeof(data)*8/pbl_time))
		print('sys.getsizeof(data) = {}'.format(sys.getsizeof(data)))
		bandwidth[str(assist_name[i])] = sys.getsizeof(data)*8/pbl_time
	return bandwidth

if __name__ == '__main__':
	# main program
	server_name = sys.argv[1]	#第一引数　サーバのIPアドレスあるいはホスト名
	server_port = int(sys.argv[2])	#第二引数　ポート番号
	file_name = sys.argv[3]		#第三引数　ファイル名
	token_str = sys.argv[4]		#第四引数　トークン文字列
	genkey_data = pbl2018.genkey(token_str)
	
	#帯域幅を測定----------------------------------------------------------
	bandwidth = bandwidth_measurement(server_name)
	#bpsの大きさでソート
	sorted_band = sorted(bandwidth.items(), key=lambda x:x[1], reverse=True)
	t = 0
	#上から３番目までの値を取る
	host = []
	bandwidth = []
	for i, j in sorted_band:
		if t < 3:
			host.append(i)
			bandwidth.append(j)
		t += 1
	print("host              = ", end = "")
	print(host)
	print("bandwidth         = ", end = "")
	print(bandwidth)

	#ファイルサイズ要求
	size_socket = socket(AF_INET, SOCK_STREAM)
	size_socket.connect((server_name, server_port))
	file_size = SIZE(size_socket, file_name)
	size_socket.close()	
	
	#assist経由でファイルサイズを要求---------------------------------------------	
#	assist_name = ['pbl1', 'pbl2', 'pbl3']		#ホスト名
#	assist_port = 52699				#ポート番号
#	for i in range(len(assist_name)):	
#		assist_socket = socket(AF_INET, SOCK_STREAM)
#		assist_socket.connect((assist_name[i], assist_port))
#		start_time = time.time()
#		size_data = ASSIST_SIZE(assist_socket, server_name, server_port, file_name)
#		end_time = time.time()
#		assist_socket.close()	
#		pbl_time = end_time - start_time
#		print('{}_time          = {}'.format(assist_name[i], pbl_time))

	#ファイル全体を要求--------------------------------------------------------
#	get_socket = socket(AF_INET, SOCK_STREAM)
#	get_socket.connect((server_name, server_port))
#	file_data = GET(get_socket, file_size, file_name, genkey_data)
#	get_socket.close()
#	print(len(file_data))
#	FILE_MAKE(file_data, file_name)

	#ファイルを分割して要求----------------------------------------------------
#	partial_num = 3		#分割数
#	head = [None]*partial_num
#	tail = [None]*partial_num
#	for i in range(partial_num):
#		head[i] = file_size//partial_num * i
#		tail[i] = file_size//partial_num * (i + 1) - 1
#		if i == thread_num*partial_num - 1:
#			tail[i] = file_size
#	file_data = ''
#	for i in range(pertital_num):
#		get_socket = socket(AF_INET, SOCK_STREAM)
#		get_socket.connect((server_name, server_port))
#		file_data += GET_PARTIAL(get_socket, server_name, server_port, file_name, genkey_data, head[i], tail[i])	
#		get_socket.close()
#	FILE_MAKE(file_data, file_name)

	#スレッドを使用し、ファイルを等分割して要求(サーバから直接)-----------------------------	
#	thread_num = 3		#スレッドの数
#	head = [None]*thread_num
#	tail = [None]*thread_num
#	for i in range(thread_num):
#		head[i] = file_size//thread_num * i
#		tail[i] = file_size//thread_num * (i + 1) - 1
#		if i == thread_num*partial_num - 1:
#			tail[i] = file_size
#	pool = [None]*thread_num
#	threads = [None]*thread_num
#	data = [None]*thread_num
#	file_data = ''
	#スレッドを用いたファイル要求開始
#	for i in range(thread_num):
#		pool[i] = ThreadPool(processes=1)
#	for i in range(thread_num):
#		threads[i] = pool[i].apply_async(THREADING, args=(server_name, server_port, file_name, genkey_data, head[i], tail[i]))
#	for i in range(thread_num):
#		data[i] = threads[i].get()
	#ファイル作成のためのデータ生成
#	for i in range(thread_num):
#		file_data += data[i]
	#ファイル作成 
#	FILE_MAKE(file_data, file_name)

	#assist経由でファイル全体を要求---------------------------------------------	
#	assist_name = 'pbl1'		#ホスト名
#	assist_port = 52601		#ポート番号
#	assist_socket = socket(AF_INET, SOCK_STREAM)
#	assist_socket.connect((assist_name, assist_port))
#	file_data = ASSIST(assist_socket, server_name, server_port, file_name, file_size, genkey_data)
#	assist_socket.close()	
#	FILE_MAKE(file_data, file_name)

	#assist経由でファイル一部分を要求(分割有り)-------------------------------------------
#	pbl = host
#	file_data = ''
#	j = 0
#	for i in pbl: 
#		assist_name = i			#
#		assist_port = 52601		#
#		assist_socket = socket(AF_INET, SOCK_STREAM)
#		assist_socket.connect((assist_name, assist_port))
#		file_data = ASSIST_PARTIAL(assist_socket, server_name, server_port, file_name, genkey_data, head[j], tail[j])
#		assist_socket.close()
#		if j == 0:
#			FILE_MAKE(file_data, file_name)
#			print(len(file_data))	
#		else :
#			FILE_ADD(file_data, file_name)
#			print(len(file_data))
#		j += 1

	#assist経由でファイル一部分を要求(分割無し)----------------------------------------
#	head = 50	#
#	tail = 80	#
#	assist_name = 'pbl1'
#	assist_port = 52601
#	file_data = '' 
#	assist_socket = socket(AF_INET, SOCK_STREAM)
#	assist_socket.connect((assist_name, assist_port))
#	file_data = ASSIST_PARTIAL(assist_socket, server_name, server_port, file_name, genkey_data, head, tail)
#	assist_socket.close()
#	print(file_data)
#	print('data_size = {}'.format(len(file_data)))

	#スレッドを使用し、ファイルを等分割して要求(assist経由)--------------------------------
#	thread_num = 3		#スレッドの個数（経由地の数）
#	head = [None]*thread_num
#	tail = [None]*thread_num
#	for i in range(thread_num):
#		head[i] = file_size//thread_num * i
#		tail[i] = file_size//thread_num * (i + 1) - 1
#		if i == thread_num*partial_num - 1:
#			tail[i] = file_size
#	pool = [None]*thread_num	
#	threads = [None]*thread_num
#	data = [None]*thread_num
#	assist_name = host			#ホスト名
#	assist_port = 52604			#ポート番号
#	file_data = ''
	#スレッドを用いたファイル要求開始	
#	for i in range(thread_num):
#		pool[i] = ThreadPool(processes=1)
#	for i in range(thread_num):
#		threads[i] = pool[i].apply_async(THREADING_ASSIST, args=(assist_name[i], assist_port, server_name, server_port, file_name, genkey_data, head[i], tail[i]))
#	for i in range(thread_num):
#		data[i] = threads[i].get()
	#ファイル作成のためのデータ生成
#	for i in range(len(data)):
#		file_data += data[i]
	#ファイル作成 
#	FILE_MAKE(file_data, file_name)

	#スレッドを使用し、ファイルを等分割して要求(assist経由)---------------------------------
#	thread_num = 3		#経由地の数
#	partial_num = 2		#分割数(それぞれの経由地に何分割して要求するか)
#	a = 0	
#	b = 0
#	head = [None]*thread_num*partial_num
#	tail = [None]*thread_num*partial_num
#	for i in range(thread_num*partial_num):
#		head[i] = file_size//(thread_num*partial_num) * i
#		tail[i] = file_size//(thread_num*partial_num) * (i + 1) - 1
#		if i == thread_num*partial_num - 1:
#			tail[i] = file_size
#	pool = [None]*thread_num*partial_num	
#	threads = [None]*thread_num*partial_num
#	data = [None]*thread_num*partial_num
#	assist_name = host				#ホスト名
#	assist_port = 52601				#ポート番号
#	file_data = ''
	#スレッドを用いたファイル要求開始	
#	for i in range(thread_num*partial_num):
#		pool[i] = ThreadPool(processes=1)
#	while a < thread_num:
#		threads[b] = pool[b].apply_async(THREADING_ASSIST, args=(assist_name[a], assist_port, server_name, server_port, file_name, genkey_data, head[b], tail[b]))
#		b += 1
#		if b % thread_num == 0:
#			a += 1
#	for i in range(thread_num*partial_num):
#		data[i] = threads[i].get()
	#ファイル作成のためのデータ生成
#	for i in range(len(data)):
#		file_data += data[i]
	#ファイル作成 
#	FILE_MAKE(file_data, 'data.txt')

	#スレッドを使用し、ファイルを分割して要求(assist経由)--------------------------------
	#帯域幅に応じてファイルを分割
#	thread_num = 3		#スレッドの個数（経由地の数）
#	rate = []
#	file_size_partial = []
#	head = [None]*thread_num
#	tail = [None]*thread_num
#	for i in bandwidth:
#		r = i/sum(bandwidth)
#		rate.append(round(r,1))
#	print("rate              = ", end = "")
#	print(rate)
#	for i in rate:
#		file_size_partial.append(int(file_size*i))
#	print("file_size_partial = ", end = "")
#	print(file_size_partial)
#	head[0] = 0
#	tail[0] = file_size_partial[0] - 1
#	head[1] = file_size_partial[0]
#	tail[1] = file_size_partial[0] + file_size_partial[1] - 1
#	head[2] = file_size_partial[0] + file_size_partial[1]
#	tail[2] = file_size
#	print("head               =", end = "")
#	print(head)
#	print("tail               =", end = "")
#	print(tail)
#	pool = [None]*thread_num	
#	threads = [None]*thread_num
#	data = [None]*thread_num
#	assist_name = host	#ホスト名
#	assist_port = 52601	#ポート番号
#	file_data = ''
	#スレッドを用いたファイル要求開始	
#	for i in range(thread_num):
#		pool[i] = ThreadPool(processes=1)
#	for i in range(thread_num):
#		threads[i] = pool[i].apply_async(THREADING_ASSIST, args=(assist_name[i], assist_port, server_name, server_port, file_name, genkey_data, head[i], tail[i]))
#	for i in range(thread_num):
#		data[i] = threads[i].get()
	#ファイル作成のためのデータ生成
#	for i in range(len(data)):
#		file_data += data[i]
	#ファイル作成 
#	FILE_MAKE(file_data, file_name)

	#スレッドを使用し、ファイルを等分割して要求(assist経由)---------------------------------
	#帯域幅に応じてファイルを分割
	thread_num = 3		#経由地の数
	partial_num = 3		#分割数(それぞれの経由地に何分割して要求するか)
	rate = []
	file_size_partial = []
	for i in bandwidth:
		r = i/sum(bandwidth)
		rate.append(round(r,1))
	print("rate              = ", end = "")
	print(rate)
	for i in rate:
		file_size_partial.append(int(file_size*i))
	print("file_size_partial = ", end = "")
	print(file_size_partial)
	a = 0	
	b = 0
	head = [None]*thread_num*partial_num
	tail = [None]*thread_num*partial_num
	head[0] = 0 
	tail[0] = file_size_partial[0] // partial_num - 1
	head[1] = file_size_partial[0] // partial_num
	tail[1] = file_size_partial[0] // partial_num * 2 - 1
	head[2] = file_size_partial[0] // partial_num * 2
	tail[2] = file_size_partial[0] - 1
	head[3] = file_size_partial[0]
	tail[3] = file_size_partial[0] + file_size_partial[1] // partial_num - 1
	head[4] = file_size_partial[0] + file_size_partial[1] // partial_num
	tail[4] = file_size_partial[0] + file_size_partial[1] // partial_num * 2 - 1
	head[5] = file_size_partial[0] + file_size_partial[1] // partial_num * 2 
	tail[5] = file_size_partial[0] + file_size_partial[1] - 1
	head[6] = file_size_partial[0] + file_size_partial[1]
	tail[6] = file_size_partial[0] + file_size_partial[1] + file_size_partial[2] // partial_num - 1
	head[7] = file_size_partial[0] + file_size_partial[1] + file_size_partial[2] // partial_num
	tail[7] = file_size_partial[0] + file_size_partial[1] + file_size_partial[2] // partial_num * 2 - 1
	head[8] = file_size_partial[0] + file_size_partial[1] + file_size_partial[2] // partial_num * 2 
	tail[8] = file_size
	pool = [None]*thread_num*partial_num	
	threads = [None]*thread_num*partial_num
	data = [None]*thread_num*partial_num
	assist_name = host				#ホスト名
	assist_port = 52605				#ポート番号
	file_data = ''
	#スレッドを用いたファイル要求開始	
	for i in range(thread_num*partial_num):
		pool[i] = ThreadPool(processes=1)
	t1 = time.time()
	while a < thread_num:
		threads[b] = pool[b].apply_async(THREADING_ASSIST, args=(assist_name[a], assist_port, server_name, server_port, file_name, genkey_data, head[b], tail[b]))
		b += 1
		if b % thread_num == 0:
			a += 1
	for i in range(thread_num*partial_num):
		data[i] = threads[i].get()
	#ファイル作成のためのデータ生成
	for i in range(len(data)):
		print(len(data[i]))
		file_data += data[i]
	print(len(file_data))
	#ファイル作成 
	FILE_MAKE(file_data, file_name)


	#ダイジェストを計算------------------------------------------------------------
	digest = pbl2018.repkey(token_str, file_name)	

	#ダイジェストを報告------------------------------------------------------------
	digest_socket = socket(AF_INET, SOCK_STREAM)
	digest_socket.connect((server_name, server_port))
	REP(digest_socket, file_name, digest)
	digest_socket.close()
	t2 = time.time()
	print('Transmission time: {}  [sec]'.format(t2-t1))

