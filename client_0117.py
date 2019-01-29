# -*- coding: utf-8 -*-
# client_0117.py
#https://stackoverflow.com/questions/6893968/how-to-get-the-return-value-from-a-thread-in-python

from socket import *
from multiprocessing.pool import ThreadPool	#並行処理使用のため	
import time
import sys
import pbl2018
import threading	#スレッド使用のため

def SIZE(client, fn):
	#要求するファイルのデータサイズを取得する関数
	client.send(('SIZE ' + fn + '\n').encode())	#ファイルのサイズを要求
	message = client.recv(1024).decode()		#メッセージを受信
	print(message)
	message = message.split()
	#受信したメッセージに応じた処理
	#OKであれば、ファイルサイズを戻り値として返す
	if message[0] == 'OK':
		return int(message[2])
	elif message[1] == 101 :
		print('Please change the file name.\n')
		sys.exit()
	else :
		print('Please rewrite the command.\n') 
		sys.exit()

def GET(client, fs, fn, gd):
	#ファイル全体を要求し、ファイルデータを受信する関数
	message = 'GET ' + fn + ' ' + gd + ' ALL\n'
	print(message)
	client.send(message.encode())	#GET要求
	#メッセージを受信
	recv_bytearray = bytearray()
	while True:
		b = client.recv(1)
		recv_bytearray.append(b[0]) 
		if b == b'\n':
			recv_str = recv_bytearray.decode()
			break
	recv_message = recv_str.split()
	print(recv_str)
	#メッセージに応じた処理
	#OKであれば、ファイルデータを受信し、それを戻り値として返す
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
	#ファイルの一部を要求する関数
	#GET要求メッセージを作成
	message = 'GET ' + fn + ' ' + gd + ' PARTIAL ' + str(head) + ' ' + str(tail) + '\n'	
	print(message)
	client.send(message.encode())	#GET要求メッセージを送信
	#送信したGET要求に対する返信メッセージを受信
	recv_bytearray = bytearray()
	while True:
		b = client.recv(1)
		recv_bytearray.append(b[0]) 
		if b == b'\n':
			recv_str = recv_bytearray.decode()
			break
	recv_message = recv_str.split()
	#返信メッセージに応じた処理
	#'OK'であれば、ファイルデータを受信し、戻り値としてそのデータを返す
	#そうでなければ、エラーメッセージを表示
	if recv_message[0] == 'OK':				
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


def FILE_MAKE(data, fn):
	#ファイルを作成する関数　
	#引数：ファイルデータ、ファイル名
	f = open(fn, 'w')
	f.write(data)
	f.close()

def FILE_ADD(data, fn):
	#ファイルに追記
	f = open(fn, 'a')
	f.write(data)
	f.close()
	
def REP(client, fn, dig):
	#受信したファイルのダイジェストをサーバに報告する関数
	client.send(('REP ' + fn + ' ' + dig + '\n').encode())	#REPメッセージを送信
	#送信したREPメッセージに対する返信を受信し、それに応じた処理
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
	#経由地にサーバのIPアドレスあるいはホスト名、ポート番号、GETメッセージ、ファイルサイズを送信する関数
	#ファイル全体を要求
	message = str(sn) + ' ' + str(sp) +  '\n'
	get = 'GET ' + fn + ' ' + gd + ' ALL\n'
	client.send((message+ ' ' + get + str(fs)).encode())
	#経由地からデータを受信し、それを戻り値として返す
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
	#経由地を通してファイルの一部を要求する関数
	message = str(sn) + ' ' + str(sp) +  '\n'	#serverのホスト名、ポート番号
	get = 'GET ' + fn + ' ' + gd + ' PARTIAL ' + str(head) + ' ' + str(tail) + '\n'	#GET要求メッセージ
	print(get)
	client.send((message + ' ' + get + str(tail - head)).encode())	#以上のメッセージを送信
	#GET要求に対して送られてきたファイルデータを受信し、それを戻り値として返す
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
	#並行処理で行う関数
	#server以外の別host経由でファイルの一部分を要求、受信し、それを戻り値として返す
	thread_name = an	
	thread_port = ap		
	thread_socket = socket(AF_INET, SOCK_STREAM)
	thread_socket.connect((thread_name, thread_port))
	data = ASSIST_PARTIAL(thread_socket, sn, sp, fn, gd, head, tail)
	thread_socket.close()
	return data

def bandwidth_measurement(client_name,server_name):
	#host1つを経由したclient, server間の帯域幅を測定する関数
	#測定結果をリストで戻り値として返す
	bandwidth = {}
	recv_bytearray = [None]*6	
	
	assist_name = ['pbl1', 'pbl2', 'pbl3', 'pbl4', 'pbl5', 'pbl6', 'pbl7']	#ホスト名
	assist_name.remove(server_name) #serverになるホスト名を除く
	assist_name.remove(client_name)	#clientになるホスト名を除く 
	for i in range(len(assist_name)):	
		assist_socket = socket(AF_INET, SOCK_STREAM)
		assist_socket.connect((assist_name[i], 52699))
		start_time = time.time() #時間計測開始
		#1500文字分のデータを要求、受信
		assist_socket.send(('assist {} {}'.format(server_name, 52699)).encode())
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
		end_time = time.time()	#時間計測終了
		pbl_time = end_time - start_time
		print('{}_time          = {} [sec]'.format(assist_name[i], pbl_time))
		print('{}_bandwidth     = {} [bps]'.format(assist_name[i], sys.getsizeof(data)*8/pbl_time))
		print('------------------------------')
		bandwidth[str(assist_name[i])] = sys.getsizeof(data)*8/pbl_time
	return bandwidth

if __name__ == '__main__':
	# main program
	client_name = sys.argv[1]	#第1引数　clientのIPアドレスあるいはホスト名
	server_name = sys.argv[2]	#第2引数　serverのIPアドレスあるいはホスト名
	server_port = int(sys.argv[3])	#第3引数　ポート番号
	file_name = sys.argv[4]		#第4引数　ファイル名
	token_str = sys.argv[5]		#第5引数　トークン文字列
	genkey_data = pbl2018.genkey(token_str)
	
	#帯域幅を測定----------------------------------------------------------
	bandwidth = bandwidth_measurement(client_name,server_name)
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
	
	#並列処理により、ファイルを分割して要求(assist経由)---------------------------------
	#帯域幅の割合に応じてファイルを分割
	rate = []
	file_size_partial = []
	for i in bandwidth:
		r = i/sum(bandwidth)
		rate.append(round(r,2))	#小数点以下第2位まで
	print("rate              = ", end = "")
	print(rate)
	for i in rate:
		file_size_partial.append(int(file_size*i))
	print("file_size_partial = ", end = "")
	print(file_size_partial)
	assist_name = host	#ホスト名
	assist_port = 52601	#ポート番号
	file_data = ''
	a = 8	#経由地１ 分割数
	b = 4	#経由地２ 分割数
	c = 3	#経由地３ 分割数
	#初期化
	pool = [None]*(a + b + c)	
	threads = [None]*(a + b + c)
	data = [None]*(a + b + c)
	for i in range(a+b+c):
		pool[i] = ThreadPool(processes=1)
	head = [None] * (a + b + c)
	tail = [None] * (a + b + c)
	#ファイルを分割し、要求するファイルデータの先頭、末尾を決定
	#head
	for i in range(a):
		head[i] = file_size_partial[0] // a * i
		if i == 0:
			head[i] = 0
	#tail
	for i in range(a):
		tail[i] = file_size_partial[0] // a * (i+1) - 1
		if i == a-1:
			tail[i] = file_size_partial[0] - 1
	#	print('head[{}] = {}'.format(i, head[i]), end = "")
	#	print('      tail[{}] = {}'.format(i, tail[i]))
	
	#head
	for i in range(b):
		head[i+a] = file_size_partial[0] + file_size_partial[1] // b * i
		if i == 0:
			head[i+a] = file_size_partial[0]
	#tail
	for i in range(b):
		tail[i+a] = file_size_partial[0] + file_size_partial[1] // b * (i+1) - 1
		if i == b-1:
			tail[i+a] = file_size_partial[0] + file_size_partial[1] - 1
	#	print('head[{}] = {}'.format(i+a, head[i+a]), end = "")
	#	print('      tail[{}] = {}'.format(i+a, tail[i+a]))
	
	#head
	for i in range(c):
		head[i+a+b] = file_size_partial[0] + file_size_partial[1]  + file_size_partial[2] // c * i
		if i == 0:
			head[i+a+b] = file_size_partial[0] + file_size_partial[1]
	#tail
	for i in range(c):
		tail[i+a+b] = file_size_partial[0] + file_size_partial[1]  + file_size_partial[2] // c * (i+1) - 1
		if i == c-1:
			tail[i+a+b] = file_size
	#	print('head[{}] = {}'.format(i+a+b, head[i+a+b]), end = "")
	#	print('      tail[{}] = {}'.format(i+a+b, tail[i+a+b]))
	t1 = time.time()  #時間計測開始
	#並列処理の準備
	for i in range(a):
		threads[i] = pool[i].apply_async(THREADING_ASSIST, args=(assist_name[0], assist_port, server_name, server_port, file_name, genkey_data, head[i], tail[i]))
	#print('------------------------------')
	for i in range(b):
		threads[i+a] = pool[i+a].apply_async(THREADING_ASSIST, args=(assist_name[1], assist_port, server_name, server_port, file_name, genkey_data, head[i+a], tail[i+a]))
	#print('------------------------------')	
	for i in range(c):
		threads[i+a+b] = pool[i+a+b].apply_async(THREADING_ASSIST, args=(assist_name[2], assist_port, server_name, server_port, file_name, genkey_data, head[i+a+b], tail[i+a+b]))

	#print(head)
	#print(tail)
	#print(threads)

	#並行処理によりファイル要求開始	
	for i in range(a+b+c):
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

