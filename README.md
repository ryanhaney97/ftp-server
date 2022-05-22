# ftp-server

A simple FTP server I made to get reaquainted with python.
Requires Python version 3.8 due to use of asyncio library (might also work on 3.7, untested).
Does NOT have proper authentication support, currently only supports anonymous login.
FTP is not a very secure protocol on its own in the first place, so use at your own risk.

Supported Operations:

	- NOOP
	- USER
	- PASS
	- QUIT
	- CWD
	- PWD
	- CDUP
	- TYPE
	- STRU
	- MODE
	- NLST
	- LIST
	- PORT
	- PASV
	- STOR

Primarily is an IPV4 server. Unknown how well it works on non-local networks, though this is not recommended due to the low security anyways.
Please note that LIST output is simply the result of calling ls (on osx/linux) or dir (on windows), so results may vary depending on platform its run on.