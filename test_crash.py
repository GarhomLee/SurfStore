import argparse
import xmlrpc.client

if __name__ == "__main__":

	parser = argparse.ArgumentParser(description="SurfStore client")
	parser.add_argument('hostport', help='host:port of the server')
	# parser.add_argument('function', help='function name')
	args = parser.parse_args()

	hostport = args.hostport
	# function_name = args.function

	try:
		client  = xmlrpc.client.ServerProxy('http://' + hostport)
		# while True:
		# 	function_name = input("Please enter next function name, or q to quit:\n")
		# 	if function_name == "q":
		# 		quit()
		# 	elif function_name == "ping":
		# 		print("ping")
		# 	elif function_name == "isLeader":
		# 		print(client.surfstore.isLeader())
		# 	elif function_name == "isCrashed":
		# 		print(client.surfstore.isCrashed())
		# 	elif function_name == "crash":
		# 		print(client.surfstore.crash())
		# 	elif function_name == "restore":
		# 		print(client.surfstore.restore())
		# 	elif function_name == "getversion":
		# 		print(client.surfstore.tester_getversion("file1.txt"))
		# 	else:
		# 		print("Not a valid name. Please try again")
		
		# Test ping
		
		print("Ping() successful")
		print("isLeader:"+client.surfstore.isLeader())
		print("isCrashed:"+client.surfstore.isCrashed())
		print("crash:"+client.surfstore.crash())
		# client.surfstore.restore()
		print("isLeader:"+client.surfstore.isLeader())
		print("isCrashed:"+client.surfstore.isCrashed())
		client.surfstore.tester_getversion("file1.txt")

		# client.surfstore.updatefile("Test.txt", 3, [1,2,3])
	except Exception as e:
		print("Client: " + str(e))