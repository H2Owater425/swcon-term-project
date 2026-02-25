from json import dumps, load
from math import trunc
from os import listdir
from posixpath import join
from random import choice, sample
from ssl import SOCK_STREAM
from sys import stderr
from time import process_time_ns
from pandas import read_csv
from socket import AF_INET, socket as Socket

BENCHMARK_NAME = input('benchmark name: ')
FILE_COUNT = 8
ADDRESS = ('127.0.0.1', 5190)
BATCH = 10000

OPERATION_READY = 0b10000000
OPERATION_HELLO = 0b00000000
OPERATION_NOP = 0b00000010
OPERATION_SET = 0b00000011
OPERATION_DEL = 0b00000100
OPERATION_GET = 0b00000101
OPERATION_OK = 0b10000010
OPERATION_VALUE = 0b10000011
OPERATION_ERROR = 0b10000100
OPERATION_QUIT = 0b11111111

def recv_exact(sock, n: int) -> bytes:
	buf = bytearray(n)
	view = memoryview(buf)
	remaining = n

	while remaining > 0:
		chunk = sock.recv_into(view[n - remaining : n])

		if chunk == 0:
			raise ConnectionError

		remaining -= chunk

	return bytes(buf)

def unpack_length(data: bytes):
	return (data[0] << 24) | (data[1] << 16) | (data[2] << 8) | data[3]

def execute(path):
	data = read_csv(path)
	data = data[data['request_io_size_bytes'] != 0][['filename', 'op_type', 'c_time', 'request_io_size_bytes']].head(131072)

	last_index = data.last_valid_index()

	set_packet = memoryview(bytearray(1 + 1 + 64 + 4 + data['request_io_size_bytes'].max()))
	get_packet = set_packet[0:1+1+64]

	set_packet[1] = 64

	with Socket(AF_INET, SOCK_STREAM) as socket:
		socket.connect(ADDRESS)

		recv_exact(socket, 4)

		socket.send(bytes([OPERATION_HELLO, 0, 0, 1]))

		socket.recv(1)

		average_latencies = []
		i = 0
		total = 0

		for index, row in data.iterrows():
			start = process_time_ns()
			set_packet[2:66] = row['filename'].encode('utf-8')

			if row['op_type'] == 'READ':
				get_packet[0] = OPERATION_GET

				socket.send(get_packet)

				header = recv_exact(socket, 5)
				value = recv_exact(socket, unpack_length(header[1:]))

				if header[0] == OPERATION_ERROR:
					message = value.decode('utf8')

					if message != 'key must exist':
						print(f"ERROR: {message}", file=stderr)
						break
			else:
				set_packet[0] = OPERATION_SET
				value_len = row['request_io_size_bytes']

				set_packet[66] = (value_len >> 24) & 0xFF
				set_packet[67] = (value_len >> 16) & 0xFF
				set_packet[68] = (value_len >> 8) & 0xFF
				set_packet[69] = value_len & 0xFF

				socket.send(set_packet[:70 + value_len])

				if socket.recv(1)[0] == OPERATION_ERROR:
					print('ERROR: ', recv_exact(socket, unpack_length(recv_exact(socket, 4))).decode('utf8'), file=stderr)

					break
			
			total += process_time_ns() - start
			i += 1

			if i % BATCH == 0:
				print(f'{index} - {index / last_index * 100:.2f}%', file=stderr)
				average_latencies.append(total / BATCH)
				i = 0
				total = 0
				print(average_latencies)

		socket.send(bytes([OPERATION_QUIT]))

		socket.recv(1)

		if i != 0:
			average_latencies.append(total / i)

	return list(map(trunc, average_latencies))

paths = []

try:
	with open('paths.json', 'r') as paths_file:
		paths = load(paths_file)
except FileNotFoundError:
	with open('paths.json', 'w') as paths_file:
		for folder in sample(listdir('data'), FILE_COUNT):
			root = join('data', folder)

			paths.append(join(root, choice(listdir(root))))

		paths_file.write(dumps(paths))

for i, path in enumerate(paths):
	with open(f'results/{BENCHMARK_NAME}-{i}.json', 'w') as json:
		print(f'{path} - {i + 1}/{len(paths)}')

		json.write(dumps(execute(path)))