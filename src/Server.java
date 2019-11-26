import org.apache.xmlrpc.*;
import java.util.*;
import java.io.*;

public class Server { 

	private Vector<String> ServerList;

	// A simple ping, simply returns True
	public boolean ping() {
		System.out.println("Ping()");
		return true;
	}

	// Given a hash value, return the associated block
	public byte[] getblock(String hashvalue) {
		System.out.println("GetBlock(" + hashvalue + ")");

		byte[] blockData = new byte[16];
		for (int i = 0; i < blockData.length; i++) {
			blockData[i] = (byte) i;
		}

		return blockData;
	}

	// Store the provided block
	public boolean putblock(byte[] blockData) {
		System.out.println("PutBlock()");

		return true;
	}

	// Determine which of the provided blocks are on this server
	public Vector hasblocks(Vector hashlist) {
		System.out.println("HasBlocks()");

		return hashlist;
	}

	// Returns the server's FileInfoMap
	public Hashtable getfileinfomap() {
		System.out.println("GetFileInfoMap()");

		// file1.dat
		Integer ver1 = new Integer(3); // file1.dat's version

		Vector<String> hashlist = new Vector<String>(); // file1.dat's hashlist
		hashlist.add("h0");
		hashlist.add("h1");
		hashlist.add("h2");

		Vector fileinfo1 = new Vector();
		fileinfo1.add(ver1);
		fileinfo1.add(hashlist);

		// file2.dat
		Integer ver2 = new Integer(5); // file2.dat's version

		Vector fileinfo2 = new Vector();
		fileinfo2.add(ver2);
		fileinfo2.add(hashlist); // use the same hashlist

		Hashtable<String, Object> result = new Hashtable<String, Object>();
		result.put("file1.dat", fileinfo1);
		result.put("file2.dat", fileinfo2);

		return result;
	}

	// Update's the given entry in the fileinfomap
	public boolean updatefile(String filename, int version, Vector hashlist) {
		System.out.println("UpdateFile(" + filename + ")");

		return true;
	}

	// PROJECT 3 APIs below

	// Queries whether this metadata store is a leader
	// Note that this call should work even when the server is "crashed"
	public boolean isLeader() {
		System.out.println("IsLeader()");
		return true;
	}

	// "Crashes" this metadata store
	// Until Restore() is called, the server should reply to all RPCs
	// with an error (unless indicated otherwise), and shouldn't send
	// RPCs to other servers
	public boolean crash() {
		System.out.println("Crash()");
		return true;
	}

	// "Restores" this metadata store, allowing it to start responding
	// to and sending RPCs to other nodes
	public boolean restore() {
		System.out.println("Restore()");
		return true;
	}

	// "IsCrashed" returns the status of this metadata node (crashed or not)
	// This method should always work, even when the node is crashed
	public boolean isCrashed() {
		System.out.println("IsCrashed()");
		return true;
	}

	public int tester_getversion(String filename) {
		System.out.println("Getting version");
		return 1;
	}

	public boolean appendEntries(int leaderID, int term, Hashtable<String, Object> fileinfomap) {
		System.out.println("appendEntries()");
		return true;
	}

	public boolean requestVote(int requestorID, int term) {
		System.out.println("requestVote()");
		return true;
	}

	public static void main (String [] args) {
		// Check if user supplied all the command line arguments required 
        if (args.length != 2) {
            System.err.println("Usage: Server configfile servernumber");
            System.exit(1);
        }

        String config = args[0];
        int servernum = Integer.parseInt(args[1]);

        try {
	        InputStream configfile = new FileInputStream(config);
	        BufferedReader buf = new BufferedReader(new InputStreamReader(configfile));

	        String line = buf.readLine();
	        String[] strs = line.split(": ");
	        int maxnum = Integer.parseInt(strs[1]);

	        int port = 0;

	        for (int i = 0 ; i < maxnum ; i++) {
	           	line = buf.readLine();

	        	if (i != servernum) {
	        		// Append to a list of servers
	        	} else {
	        		strs = line.split(": ");
	        		String[] h = strs[1].split(":");
	        		port = Integer.parseInt(h[1]);
	        	}
	        }

			System.out.println("Attempting to start XML-RPC Server...");

			WebServer server = new WebServer(port);
			server.addHandler("surfstore", new Server());
			server.start();

			System.out.println("Started successfully.");
			System.out.println("Accepting requests. (Halt program to stop.)");

	    } catch (IOException exception) {
	    	System.err.println("Server: " + exception);
	    } catch (Exception exception){
			System.err.println("Server: " + exception);
		}
	}
}
