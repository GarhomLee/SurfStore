import org.apache.xmlrpc.*;
import java.util.*;
import java.io.*;

/** This class is a front layer for interacting with a SurfStore server */
public class Server {
	private BlockService blockService; // handle all operations about blocks
	// private MetadataService metadataService; // handle metadata
	private StatusManager statusManager; // manage the different statuses of a server and its corresponding
											// behaviors

	// private Hashtable<String, byte[]> blockMap; // store key-value pairs of
	// hash->byte blocks
	// private Hashtable<String, Vector<Object>> fileInfoMap; // store key-value
	// pairs of filename->Vector, whichcontains version and hash list (another
	// Vector)
	private static String currentNode; // "host:port" of current node
	private static Vector<String> serversList; // record all other servers' "host:port" info
	// private static int PORT = 50000; // for local test
	// private static int PORT = 8080; // for gradescope

	public Server(String currentNode, Vector<String> serversList) {
		// blockMap = new Hashtable<>();
		// fileInfoMap = new Hashtable<>();
		// blockService = new BlockService(blockMap);
		// metadataService = new MetadataService(fileInfoMap);
		this.blockService = new BlockService();
		// this.metadataService = new MetadataService();
		this.statusManager = new StatusManager(currentNode, serversList);
	}

	/* the following APIs can be called by the clients */

	// Given a hash value, return the associated block
	public byte[] getblock(String hashvalue) {
		// System.err.println("getblock() of " + hashvalue); // debug
		return blockService.getblock(hashvalue);
	}

	// Store the provided block
	public boolean putblock(byte[] blockData) {
		// System.err.println("putblock() called."); // debug
		return blockService.putblock(blockData);
	}

	// Determine which of the provided blocks are on this server
	public Set<String> hasblocks(Vector<String> hashlist) {
		System.err.println("hasblocks() called."); // debug
		return blockService.hasblocks(hashlist);
	}

	// Returns the server's FileInfoMap
	public Hashtable<String, Vector<Object>> getfileinfomap() throws XmlRpcException {
		try {
			// throw exceptions
			if (statusManager.isCrashed()) {
				throw new XmlRpcException(-1, "This server is crashed. Please try another server.");
			}
			if (!statusManager.isLeader()) {
				throw new XmlRpcException(-2, "This server is not the leader. Please contact the leader server.");
			}
			return statusManager.getfileinfomap();
		} catch (Exception exception) {
			// catch an exception of crashed server
			System.err.println("Exception on Server.getfileinfomap() is found (might be that the leader is crashed): ");
			System.err.println(exception);
			// throw exception back to clinet
			throw new XmlRpcException(-1, "This server is crashed. Please try another server.");
		}
	}

	// Update's the given entry in the fileinfomap
	public boolean updatefile(String filename, int version, Vector<String> hashlist) throws XmlRpcException {
		try {
			// throw exceptions
			if (statusManager.isCrashed()) {
				// the server is crashed
				throw new XmlRpcException(-1, "This server is crashed. Please try another server.");
			}
			if (!statusManager.isLeader()) {
				// the server is not the leader
				throw new XmlRpcException(-2, "This server is not the leader. Please contact the leader server.");
			}

			// get the result back if the leader server is functional
			return statusManager.updatefile(filename, version, hashlist);
		} catch (Exception exception) {
			// catch an exception of crashed server
			System.err.println("Exception on Server.updatefile() is found (might be that the leader is crashed): ");
			System.err.println(exception);
			// throw exception back to clinet if it is crashed
			throw new XmlRpcException(-1, "This server is crashed. Please try another server.");
		}
	}

	// A simple ping, simply returns True
	public boolean ping() {
		System.err.println("debug: Ping()");
		return true;
	}

	// PROJECT 3 APIs below: they are not called by the clients

	/* start for leader election and log replication implementing Raft protocol */
	private void run() {
		statusManager.run();
	}

	// Queries whether this metadata store is a leader
	// Note that this call should work even when the server is "crashed"
	public boolean isLeader() {
		return statusManager.isLeader();
	}

	// "Crashes" this metadata store
	// Until Restore() is called, the server should reply to all RPCs
	// with an error (unless indicated otherwise), and shouldn't send
	// RPCs to other servers
	public boolean crash() {
		return statusManager.crash();
	}

	// "Restores" this metadata store, allowing it to start responding
	// to and sending RPCs to other nodes
	public boolean restore() {
		return statusManager.restore();
	}

	// "IsCrashed" returns the status of this metadata node (crashed or not)
	// This method should always work, even when the node is crashed
	public boolean isCrashed() {
		return statusManager.isCrashed();
	}

	/*
	 * Return the version of the given file even when the server is crashed. Return
	 * 0 if it does not exist.
	 */
	public int tester_getversion(String filename) {
		return statusManager.getFileVersion(filename);
	}

	/** start a server */
	public static void main(String[] args) {
		try {
			// Check if user supplied all the command line arguments required
			if (args.length != 2) {
				System.err.println("Usage: Server configfile servernumber");
				System.exit(1);
			}

			String config = args[0];
			int servernum = Integer.parseInt(args[1]); // indicate the current server at the list
			// open config file
			InputStream configfile = new FileInputStream(config);
			BufferedReader buf = new BufferedReader(new InputStreamReader(configfile));

			// read the first line of config file to get the max num of servers
			String line = buf.readLine();
			String[] strs = line.split(": ");
			int maxnum = Integer.parseInt(strs[1]);

			// check if the servernum is valid
			if (servernum >= maxnum) {
				System.err.printf("The server id should not be equal to or greater than the max num: %d.\n", maxnum);
				System.exit(1);
			}

			int port = 0;
			serversList = new Vector<>(); // initialize the servers list
			for (int i = 0; i < maxnum; i++) {
				line = buf.readLine();
				strs = line.split(": ");
				String ip = strs[1]; // get "host:port"

				if (i != servernum) {
					// Append to a list of servers
					serversList.add(ip);
				} else {
					currentNode = ip;
					// get the port of current server
					String[] h = ip.split(":");
					port = Integer.parseInt(h[1]);
				}
			}

			// start a server to receive RPC calls
			System.err.println("Attempting to start XML-RPC Server...");

			WebServer webServer = new WebServer(port);
			Server surfstoreServer = new Server(currentNode, serversList); // create an instance
			webServer.addHandler("surfstore", surfstoreServer);
			webServer.start(); // start to receive RPC calls

			System.err.println("Server started successfully: running server " + servernum + " at port " + port);
			System.err.println("Accepting requests. (Halt program to stop.)");
			// System.err.println("program version: proj2/proj2-cse224/, branch: master");
			// // for proj-2
			System.err.println("program version: SurfStore/proj3-java/, branch: log-replication"); // for proj-3
			System.err.println();

			// start for leader election and log replication implementing Raft protocol
			surfstoreServer.run();

		} catch (Exception exception) {
			// catch XmlRpcException
			System.err.println("Exception on Server is found: ");
			System.err.println(exception);
		}
	}
}
