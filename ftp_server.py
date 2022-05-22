import asyncio
import socket
import sys
import os
import os.path
import stat
import subprocess
from random import randint
from types import CoroutineType

verbmap = {}
base_prefix = ""
min_pasv_port = 1024
max_pasv_port = 1124
real_address4 = None
real_address6 = None
used_ports = set()

LIMITER = ((1 << 21) - 1)
def char_hash(key, m):
	"Unused for now"
	return (1296111 * (key ^ (key >> (21-m)))) & LIMITER >> (21-m)

def _str_counting_sort(s, start, n, k):
	while((n-start) > 1):
		largest_char = 0
		for i in range(start, n):
			ic = ord(s[i][k])
			if(ic > largest_char):
				largest_char = ic
		char_count = [0] * (largest_char+1)
		mode = 1
		for i in range(start, n):
			char_count[ord(s[i][k])] += 1
		for i in range(2, largest_char+1):
			if(char_count[mode] < char_count[i]):
				mode = i
		if(char_count[mode] < n):
			#top/bucket[i] = sum of all char_count up to position i-1 plus start
			bucket = [0] * (largest_char+1)
			top = [0] * (largest_char+1)
			bucket[0] = top[0] = start
			for i in range(1, largest_char+1):
				top[i] = bucket[i] = bucket[i-1] + char_count[i-1]
			for i in range(largest_char+1):
				while(top[i] < bucket[i] + char_count[i]):
					if(ord(s[top[i]][k]) == i):
						top[i]+=1
					else:
						j = ord(s[top[i]][k])
						i1 = top[i]
						i2 = top[j]
						top[j] += 1
						temp = s[i1]
						s[i1] = s[i2]
						s[i2] = temp
			for i in range(1, largest_char+1):
				if(i != mode):
					_str_counting_sort(s, bucket[i], char_count[i], k+1)
			start = bucket[mode]
			n = char_count[mode]
			k += 1
		else:
			k += 1


def str_radix_sort(str_list):
	_str_counting_sort(str_list, 0, len(str_list), 0)

def str_kcmp(s1, s2, k):
	"Compares the characters at position k in s1 and s2 if they exist. Primarily used for bounds checking character comparisons."
	s1k = ord(s1[k]) if k < len(s1) else 0
	s2k = ord(s2[k]) if k < len(s2) else 0
	return s1k - s2k

def _str_quicksort_sub(coll, start, end, k):
	while (n:=end-start+1) > 3:
		if(n > 3):
			if(str_kcmp(coll[end], coll[start], k) < 0):
				coll[end], coll[start] = coll[start], coll[end]
			mid = (n >> 1) + start #(n >> 1) is a quicker version of of n//2 for positive integers. Since n is a length and, therefore, always a positive integer, this will work.
			if(str_kcmp(coll[mid], coll[start], k) < 0):
				coll[mid], coll[start] = coll[start], coll[mid]
			if(str_kcmp(coll[end], coll[mid], k) < 0):
				coll[mid], coll[end] = coll[end], coll[mid]
			lt = start
			gt = end
			pivot = coll[mid]
			i = start+1
			while i < gt:
				scmp = str_kcmp(coll[i], pivot, k)
				if(scmp < 0):
					lt+=1
					coll[i], coll[lt] = coll[lt], coll[i]
					i+=1
				elif(scmp > 0):
					gt-=1
					coll[i], coll[gt] = coll[gt], coll[i]
				else:
					i+=1
			#Tail recur on largest subset
			ltn = lt-start
			gtn = end-gt
			eqn = gt-lt
			if(ltn > eqn):
				_str_quicksort_sub(coll, lt+1, gt-1, k+1)
				if(ltn > gtn):
					_str_quicksort_sub(coll, gt, end, k)
					end = lt
				else:
					_str_quicksort_sub(coll, start, lt, k)
					start = gt
			else:
				_str_quicksort_sub(coll, start, lt, k)
				if(eqn > gtn):
					_str_quicksort_sub(coll, gt, end, k)
					start = lt+1
					end = gt-1
					k+=1
				else:
					_str_quicksort_sub(coll, lt+1, gt-1, k+1)
					start = gt
	if(n > 1):
		if(coll[end][k:] < coll[start][k:]):
			coll[end], coll[start] = coll[start], coll[end]
		if(n > 2):
			mid = start + 1
			if(coll[mid][k:] < coll[start][k:]):
				coll[mid], coll[start] = coll[start], coll[mid]
			if(coll[end][k:] < coll[mid][k:]):
				coll[mid], coll[end] = coll[end], coll[mid]


def str_quicksort(coll):
	_str_quicksort_sub(coll, 0, len(coll) - 1, 0)

class FTPConnection:
	def __init__(self, reader, writer):
		self.reader = reader
		self.writer = writer
		self.username = None
		self.logged_in = False
		self.prefix = base_prefix
		self.port_addr = None
		self.data_connection = None
		self.data_server = None
		self.binary = False
		self.data_connect_event = asyncio.Event()
	def write_response(self, code):
		self.writer.write(str(code).encode()+b"\r\n")
	def decode_pathname(self, pathname):
		replaced_path = pathname.replace("\"\"", "\"").replace("\000", "\012")
		if(replaced_path[0] == os.sep):
			return replaced_path
		else:
			return os.path.join(self.prefix, replaced_path)
	def encode_pathname(self, pathname):
		return pathname.replace("\"", "\"\"").replace("\012", "\000")
	async def clear_data_connection(self):
		if(self.data_connection is not None):
			self.data_connection[1].close()
			await self.data_connection[1].wait_closed()
			self.data_connection = None
		if(self.data_server is not None):
			self.data_server.close()
			await self.data_server.wait_closed()
			self.data_server = None
		self.data_connect_event.clear()
		self.port_addr = None
	async def binary_type(self, parameter):
		parameter=parameter.upper()
		if(parameter=="A" or parameter=="A N"):
			self.binary = False
			return 200
		if(parameter=="I" or parameter=="L 8"):
			self.binary = True
			return 200
		return 504
	async def handle_stru(self, parameter):
		parameter=parameter.upper()
		if(parameter=="F"):
			return 200
		return 504
	async def handle_mode(self, parameter):
		parameter=parameter.upper()
		if(parameter=="S"):
			return 200
		return 504
	async def read_request(self):
		data = await self.reader.readline()
		message = data.decode().strip().split(" ", maxsplit=1)
		if(len(message) == 1):
			message += [""]
		return message
	async def user(self, username):
		self.username = username
		self.logged_in = False
		return 331
	async def password(self, password):
		if self.username is None or self.logged_in:
			return 503
		if self.username == "anonymous" and "@" in password:
			self.logged_in = True
			return 230
		else:
			self.username = None
			return 530
	async def port(self, addr):
		await self.clear_data_connection()
		try:
			split_addr = addr.rsplit(",", 2)
			if(len(split_addr)!=3):
				return "504 Invalid address: " + addr
			addr = split_addr[0].replace(",", ".")
			p1 = int(split_addr[1])
			p2 = int(split_addr[2])
			portnum = p1*256+p2
			self.port_addr = (addr, portnum)
			print(self.port_addr)
			return 200
		except:
			return "504 Invalid address: " + addr
	async def data_server_callback(self, reader, writer):
		if self.data_connection is None:
			self.data_connection = (reader, writer)
			self.data_connect_event.set()
		else:
			writer.close()
			await writer.wait_closed()
	async def pasv(self, _):
		global min_pasv_port, max_pasv_port, used_ports, real_address4
		await self.clear_data_connection()
		while((chosen_port := randint(min_pasv_port, max_pasv_port)) in used_ports):
			pass
		used_ports.add(chosen_port)
		self.data_server = await asyncio.start_server(self.data_server_callback, port=chosen_port, family=socket.AF_INET)
		response = real_address4.replace(".",",") + "," + str(chosen_port//256) + "," + str(chosen_port%256)
		return "227 "+response
	async def establish_data_connection(self):
		if(self.port_addr is not None):
			try:
				self.data_connection = await asyncio.open_connection(host=self.port_addr[0], port=self.port_addr[1])
				self.port_addr = None
			except:
				await self.clear_data_connection()
				return "425 Failed to connect to PORT."
		if(self.data_server is not None):
			try:
				await asyncio.wait_for(self.data_connect_event.wait(), 10)
			except asyncio.TimeoutError:
				await self.clear_data_connection()
				return "425 Failed to connect - connection timeout"
	async def retr(self, encoded_filepath):
		filepath = self.decode_pathname(encoded_filepath)
		if(not os.path.isfile(filepath)):
			await self.clear_data_connection()
			return "550 File does not exist."
		self.write_response(150)
		error = await self.establish_data_connection()
		if(error is not None):
			return error
		if(self.data_connection is not None):
			data_writer = self.data_connection[1]
			if(self.binary):
				loop = asyncio.get_running_loop()
				try:
					with open(filepath, mode="rb") as file:
						bytes_sent = await loop.sendfile(data_writer.transport, file)
				except BaseException as err:
					await self.clear_data_connection()
					return (f"451 Disc read failed. {err=}, {type(err)=}")
			else:
				try:
					with open(filepath, mode="rt", errors="ignore") as file:
						while((linein:=file.readline())!=""):
							data_writer.write((linein.strip()+"\r\n").encode())
				except BaseException as err:
					await self.clear_data_connection()
					return (f"451 Disc read failed. {err=}, {type(err)=}")
			await data_writer.drain()
			await self.clear_data_connection()
			return 226
		else:
			await self.clear_data_connection()
			return "425 No data connection established."
	async def stor(self, encoded_filepath):
		filepath = self.decode_pathname(encoded_filepath)
		self.write_response(150)
		error = await self.establish_data_connection()
		if(error is not None):
			return error
		if(self.data_connection is not None):
			data_reader = self.data_connection[0]
			if(self.binary):
				try:
					with open(filepath, mode="wb") as file:
						while((read_data:=await data_reader.read(100))!=b""):
							file.write(read_data)
				except BaseException as err:
					await self.clear_data_connection()
					return (f"451 Disc write failed. {err=}, {type(err)=}")
			else:
				try:
					with open(filepath, mode="wt", errors="ignore") as file:
						while((read_data:=await data_reader.readline())!=b""):
							file.write(read_data.decode(errors="ignore"))
				except BaseException as err:
					await self.clear_data_connection()
					return (f"451 Disc read failed. {err=}, {type(err)=}")
			await self.clear_data_connection()
			return 226
		else:
			await self.clear_data_connection()
			return "425 No data connection established."
	async def nlst(self, encoded_filepath):
		if(encoded_filepath == ""):
			filepath = self.prefix
		else:
			filepath = self.decode_pathname(encoded_filepath)
			if(os.path.isfile(filepath)):
				await self.clear_data_connection()
				return "504 NLST Does not accept file parameters."
			elif(not os.path.isdir(filepath)):
				await self.clear_data_connection()
				return "550 Path does not exist."
		self.write_response(150)
		error = await self.establish_data_connection()
		if(error is not None):
			return error
		if(self.data_connection is not None):
			data_writer = self.data_connection[1]
			dirlist = os.listdir(filepath)
			str_quicksort(dirlist)
			for entry in dirlist:
				data_writer.write(self.encode_pathname(entry).encode() + b"\r\n")
			await data_writer.drain()
			await self.clear_data_connection()
			return 226
		else:
			await self.clear_data_connection()
			return "425 No data connection established."
	def make_EPLF_response(self, filepath):
		stat_result = os.stat(filepath)
		response = "+"
		if(os.path.isfile(filepath)):
			response += f"r,s{stat_result.st_size},"
		elif(os.path.isdir(filepath)):
			response += f"/,"
		response += f"i{stat_result.st_dev}.{stat_result.st_ino},m{stat_result.st_mtime},\t"
		if(filepath[-1] == "/"):
			filename = self.encode_pathname(os.path.basename(filepath[:-1]))
		else:
			filename = self.encode_pathname(os.path.basename(filepath))
		response += filename + "\r\n"
		return response
	# def make_binls_response(self, filepath):
	# 	stat_result = os.stat(filepath)
	# 	if(filepath[-1] == "/"):
	# 		filename = self.encode_pathname(os.path.basename(filepath[:-1]))
	# 	else:
	# 		filename = self.encode_pathname(os.path.basename(filepath))
	# 	response = f"{stat.filemode(stat_result.st_mode)}"
	async def list_command(self, encoded_filepath):
		if(encoded_filepath == ""):
			filepath = self.prefix
		else:
			filepath = self.decode_pathname(encoded_filepath)
		self.write_response(150)
		error = await self.establish_data_connection()
		if(error is not None):
			return error
		if(self.data_connection is not None):
			data_writer = self.data_connection[1]
			if(os.name == "posix"):
				with os.popen(f"ls -l {filepath}") as stream:
					while((linein:=stream.readline())!=""):
						data_writer.write(linein.strip().encode() + b"\r\n")
			elif(os.name == "nt"):
				with os.popen(f"dir {filepath}") as stream:
					while((linein:=stream.readline())!=""):
						data_writer.write(linein.strip().encode() + b"\r\n")
			else:
				if(os.path.isfile(filepath)):
					response = self.make_EPLF_response(filepath)
					data_writer.write(response.encode())
				if(not os.path.isdir(filepath)):
					await self.clear_data_connection()
					return "550 Path does not exist"
				else:
					dirlist = [d.path for d in os.scandir(filepath)]
					str_quicksort(dirlist)
					for entry in dirlist:
						response = self.make_EPLF_response(entry)
						data_writer.write(response.encode())
			await data_writer.drain()
			await self.clear_data_connection()
			return 226
		else:
			await self.clear_data_connection()
			return "425 No data connection established."
	async def change_working_directory(self, pathname):
		if(pathname == ""):
			return "550 No argument provided."
		decoded = self.decode_pathname(pathname)
		if (not os.path.isdir(decoded)):
			return "550 Directory does not exist."
		self.prefix = os.path.abspath(decoded)
		return 250
	async def cdup(self, _):
		new_prefix = self.prefix.rsplit(os.sep, 1)[0]
		if(new_prefix == ""):
			return 200
		self.prefix = new_prefix
		return 200
	async def print_working_directory(self, _):
		return "257 \"" + self.encode_pathname(self.prefix) + "\""
	async def noop(self, *_):
		return 200
	async def quit(self, *_):
		try:
			await self.clear_data_connection()
			self.write_response(221)
			await self.writer.drain()
		finally:
			self.writer.close()
			await self.writer.wait_closed()
	async def run_request_loop(self):
		try:
			while not self.writer.is_closing():
				while not self.logged_in:
					verb, arg = await self.read_request()
					print(">", verb, " ", arg, sep="")
					if verb == "USER":
						response = await self.user(arg)
					elif verb == "PASS":
						response = await self.password(arg)
					elif verb == "QUIT":
						await self.quit()
						return
					else:
						self.username = None
						self.logged_in = False
						if verb in verbmap_unverrified:
							response = await verbmap_unverrified[verb](self, arg)
						elif verb in verbmap:
							response = "530 Permission Denied"
						else:
							response = "502 Verb " + verb + " unsupported."
					if response is not None:
						print(response)
						self.write_response(response)
				verb, arg = await self.read_request()
				print(self.username, ">", verb, " ", arg, sep="")
				if(verb in verbmap):
					response = await verbmap[verb](self, arg)
				else:
					response = "502 Verb " + verb + " unsupported."
				if response is not None:
					print(response)
					self.write_response(response)
		except ConnectionResetError:
			print("Connection Closed")
			return

verbmap_unverrified = {
	"NOOP": FTPConnection.noop}

verbmap = {
	"USER": FTPConnection.user,
	"QUIT": FTPConnection.quit,
	"CWD": FTPConnection.change_working_directory,
	"PWD": FTPConnection.print_working_directory,
	"CDUP": FTPConnection.cdup,
	"TYPE": FTPConnection.binary_type,
	"STRU": FTPConnection.handle_stru,
	"MODE": FTPConnection.handle_mode,
	"PORT": FTPConnection.port,
	"RETR": FTPConnection.retr,
	"PASV": FTPConnection.pasv,
	"STOR": FTPConnection.stor,
	"NLST": FTPConnection.nlst,
	"LIST": FTPConnection.list_command}

async def make_connection(reader, writer):
	connection = FTPConnection(reader, writer)
	connection.write_response(220)
	await connection.run_request_loop()
	del connection

async def make_server(callback, port):
	server = await asyncio.start_server(callback, port=port, family=socket.AF_INET)
	return server

def run_server(callback, port):
	async def server_main():
		print("Starting Server...")
		server = await make_server(callback, port)
		print("Server Started")
		async with server:
			await server.serve_forever()
	asyncio.run(server_main())

def main(args):
	global base_prefix, real_address4, real_address6
	real_address4 = socket.gethostbyname(socket.gethostname())
	if(len(args)>1):
		if(not os.path.isdir(args[1])):
			print("Invalid file path provided.")
			return
		base_prefix = os.path.abspath(args[1])
	else:
		base_prefix = os.getcwd()
	print(f"Started server in {base_prefix} at address {real_address4}:21")
	run_server(make_connection, 21)

if __name__ == "__main__":
	main(sys.argv)