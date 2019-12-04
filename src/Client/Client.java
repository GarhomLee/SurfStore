import java.util.*;
import org.apache.xmlrpc.*;

public class Client {
	public static void main(String[] args) {
		// check if argument number is correct
		if (args.length != 3) {
			System.err.println("Usage: Client host:port /basedir blockSize");
			System.exit(1);
		}

		// check if input /basedir is valid
		if (!args[1].startsWith("/") || args[1].indexOf(" ") >= 0) {
			System.err.println("Invalid /basedir argument. Should start with \"/\" and cannot contain space.");
			System.exit(1);
		}

		// check if input blockSize is valid
		int blockSize = 0;
		try {
			blockSize = Integer.parseInt(args[2]);
			if (blockSize < 0) {
				System.err.println("Block size is not valid.");
				System.exit(1);
			}
		} catch (NumberFormatException e) {
			System.err.println("Block size is not valid: " + e);
			System.exit(1);
		}

		try {
			String rpcCall = "http://" + args[0] + "/RPC2";
			XmlRpcClient client = new XmlRpcClient(rpcCall);

			// Test connection
			Vector<Object> params = new Vector<>();
			boolean isPing = (boolean) client.execute("surfstore.ping", params);
			if (isPing) {
				System.err.println("Connection to the server established.");
			} else {
				System.err.println("Cannot connect to the server.");
				System.exit(1);
			}

			SyncHandler syncHandler = new SyncHandler(client, args[1], blockSize);
			syncHandler.handle();

		} catch (Exception exception) {
			System.err.println("Exception on Client is found: ");
			System.err.println(exception);
		}
	}
}
