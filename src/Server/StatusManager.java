import org.apache.xmlrpc.*;
import java.util.*;

/**
 * This class is for managing the different statuses of a server and its
 * corresponding behaviors
 */
class StatusManager {
	private String currentNode; // "host:port" of current node
	private Vector<String> serversList; // record all other servers' "host:port" info
	private Status currentStatus;
	private MetadataService metadataService; // handle metadata
	private int currentTerm; // latest term server has seen (initialized to 0 on first boot, increases
								// monotonically)
	private String votedFor; // candidate server that received vote in a given term (or null if none), only
								// valid in election period. Reset null in a new term
	private HashSet<String> votedSet;  // record servers that has voted, either "for" or "against"
	private HashSet<String> votedForSet;  // record servers that has voted "for"
	private Vector<Vector<Object>> logs; // log entries; each entry contains:
											// (1) the term when entry was received by leader (first index is 0)
											// (2) a state machine (update of a single file)
	private int commitIndex; // index of highest log entry known to be committed (initialized to -1,
								// increases monotonically)
	private Hashtable<String, Integer> nextIndex; // for each server, index of the next log entry to send to that server
													// (initialized to leader last log index + 1)

	private String currentLeader; // indicate the current leader in this term
	private int totalNodesNum; // the total number of nodes, including current node itself
	private Timer timer;
	private TimerTask task;
	private Timer requestVoteTimer;
	private TimerTask requestVoteTask;
	private int timeout; // timeout duration, depending on random number
	private Random random; // introduce random duration for timeout
	private boolean isCrashed; // indicate if this node is in the crashed state

	// for local test
	final private int TIMEOUT_DURATION = 1000; // timeout duration in milliseconds
	final private int HEARTBEAT_DURATION = 100; // heartbeat duration in milliseconds
	// for gradescope
	// final private int TIMEOUT_DURATION = 150; // timeout duration in milliseconds
	// final private int HEARTBEAT_DURATION = 30; // heartbeat duration in milliseconds

	/* all possible statuses of a node */
	public enum Status {
		LEADER, CANDIDATE, FOLLOWER;
	}

	/* constructor */
	public StatusManager(String currentNode, Vector<String> serversList) {
		this.currentNode = currentNode;
		this.serversList = serversList;
		// this.metadataService = new MetadataService();
		this.currentStatus = Status.FOLLOWER;
		this.totalNodesNum = serversList.size() + 1;
		this.votedSet = new HashSet<>();
		this.votedForSet = new HashSet<>();
		this.currentTerm = 0;
		this.logs = new Vector<>();
		this.commitIndex = -1;
		this.nextIndex = new Hashtable<>();
		this.random = new Random();
		this.isCrashed = false;
		this.requestVoteTimer = new Timer("RequestVoteTimer");
	}

	/* entrance funciton to start a new server */
	public void run() {
		// begin as a follower
		registerFollowerTimeout();
		// // for debug
		// while (true) {
		// counter++;
		// if (counter %100000000 == 0) {
		// System.err.println(counter);
		// }

		// }
	}

	/* start a new term and election */
	private void startElection() {
		System.err.println("Current term " + currentTerm + " of server " + currentNode + " has timed out.");
		// register a timeout for this election period
		task = new TimerTask() {
			public void run() {
				System.out.println(
						"Thread's name: " + Thread.currentThread().getName() + ". Task performed on: " + new Date()); // for
																														// debug
				// start a new term and election if this election period has timed out
				if (currentStatus == Status.CANDIDATE && !isCrashed) {
					startElection();
				}
			}
		};
		if (timer != null) {
			timer.cancel(); // terminates this timer, discarding any currently scheduled tasks.
		}
		timer = new Timer("ElectionTimer");
		timeout = getRandomTimeout(TIMEOUT_DURATION); // for debug
		System.err
				.println("A startElection timer is registered. currentNode: " + currentNode + ", term: " + currentTerm); // for
																															// debug
		timer.schedule(task, timeout); // timeout start

		currentTerm += 1;
		System.err.println("Election for new term " + currentTerm + " has started. currentNode: " + currentNode
				+ ", term: " + currentTerm);
		currentLeader = null;
		currentStatus = Status.CANDIDATE;
		votedSet.clear(); // reset to empty set
		votedSet.add(currentNode); // add itself
		votedForSet.clear();
		votedForSet.add(currentNode);
		votedFor = currentNode; // vote for itself

		// request votes from all other nodes if it is still a candidate and not crashed
		if (currentStatus == Status.CANDIDATE && !isCrashed) {
			sendRequestVote();
		}
	}

	/* send async requests for election votes */
	private void sendRequestVote() {
		requestVoteTask = new TimerTask() {
			public void run() {
				System.out.println(
						"Thread's name: " + Thread.currentThread().getName() + ". Task performed on: " + new Date()); // for
																														// debug
				// check if it is still a candicate
				if (currentStatus == Status.CANDIDATE) {
					if (votedSet.size() < totalNodesNum / 2 + 1) {
						// resend requestVote()
						System.err.println("Only " + votedSet.size() + " out of " + totalNodesNum
								+ " respond, with " + votedForSet.size() +" positive votes. Resend requests for votes. currentNode: " + currentNode
								+ ", term: " + currentTerm); // for debug
						if (!isCrashed) {
							sendRequestVote();
						}
					} else {
						// become a leader
						currentStatus = Status.LEADER;
						currentLeader = currentNode;
						votedFor = null;
						System.err.println("[" + currentNode + "] is now the leader with " + votedForSet.size()
								+ " votes. Current term is: " + currentTerm);
						if (!isCrashed) {
							leaderActions();
						}
					}
				}
			}
		};
		
		System.err.println("A requestVoteTimer is registered. currentNode: " + currentNode + ", term: " + currentTerm); // for debug
		requestVoteTimer.cancel();
		requestVoteTimer = new Timer("RequestVoteTimer");
		requestVoteTimer.schedule(requestVoteTask, getRandomTimeout(HEARTBEAT_DURATION)); // timeout start

		// use RPC to request votes from all other nodes
		try {
			System.err.println("Sending voting request... currentNode: " + currentNode + ", term: " + currentTerm); // for
																													// debug
			for (String server : serversList) {
				// skip the servers if it is voted for (not against) this server
				if (votedSet.contains(server))
					continue;

				XmlRpcClient client = new XmlRpcClient("http://" + server + "/RPC2");
				Vector<Object> params = new Vector<>();
				params.add(currentNode); // requestor
				params.add(currentTerm); // requestorTerm
				int requestorLastLogIndex = logs.size() - 1;
				params.add(requestorLastLogIndex); // requestorLastLogIndex
				int requestorLastLogTerm = logs.isEmpty() ? 0 : (int) logs.get(requestorLastLogIndex).get(0);
				params.add(requestorLastLogTerm); // requestorLastLogTerm
				client.executeAsync("surfstore.requestVote", params, new RequestVoteCallback(votedSet, votedForSet));
			}
		} catch (Exception exception) {
			System.err.println("Exception on StatusManager.sendRequestVote() is found: ");
			System.err.println(exception);
		}
	}

	/*
	 * Used to implement leader election. Return: (1)if it grants the vote to the
	 * request; (2) the currnet server If the server is crashed, it should return an
	 * “isCrashed” error; procedure has no effect if server is crashed
	 */
	public Vector<Object> requestVote(String requestor, int requestorTerm, int requestorLastLogIndex,
			int requestorLastLogTerm) {
		System.err.println(
				"requestVote() of: " + currentNode + ", requested by: " + requestor + ", for term: " + requestorTerm); // debug
		int lastLogIndex = logs.size() - 1;
		int lastLogTerm = logs.isEmpty() ? 0 : (int) logs.get(lastLogIndex).get(0);

		// determine if it should grant vote to the requestor
		boolean isVoted = !isCrashed; // cannot reply back if it is crashed
		System.err.println("isVoted: if it is not crashed : " + isVoted); // debug
		isVoted = isVoted && requestorTerm > currentTerm; // check term
		if (isVoted) {
			// term should update, and change into follower
			currentTerm = requestorTerm;
			currentStatus = Status.FOLLOWER;
			votedFor = requestor; // grant vote to the requestor
			registerFollowerTimeout();
		}
		System.err.println("isVoted: if it is valid term comparison: " + isVoted); // debug
		isVoted = isVoted && (lastLogTerm < requestorLastLogTerm
				|| (lastLogTerm == requestorLastLogTerm && lastLogIndex <= requestorLastLogIndex)); // check last log
		System.err.println("isVoted: if it is valid log comparion : " + isVoted); // debug
		isVoted = isVoted && (votedFor == null || votedFor.equals(requestor)); // check if it has granted vote
		System.err.println("isVoted: if it is valid voterFor : " + isVoted); // debug

		// organize the result
		Vector<Object> result = new Vector<>();
		result.add(isVoted);
		result.add(currentNode);

		if (isVoted) {
			System.err.println("This vote is granted. currentNode: " + currentNode + ", term: " + currentTerm);
			votedFor = requestor; // grant vote to the requestor
		} else {
			System.err.println("This vote is denied. currentNode: " + currentNode + ", term: " + currentTerm);
		}
		return result;
	}

	/* register follower timeout */
	private void registerFollowerTimeout() {
		// cancel the next request vote schedule if it is already a follower
		requestVoteTimer.cancel();  
		
		task = new TimerTask() {
			public void run() {
				System.out.println(
						"Task performed on: " + new Date() + "\nThread's name: " + Thread.currentThread().getName()); // for
																														// debug
				// start a new term and election if this follower period has timed out
				if (!isCrashed) {
					startElection();
				}

			}
		};
		timeout = getRandomTimeout(TIMEOUT_DURATION); // for debug
		if (timer != null) {
			timer.cancel();
		}
		timer = new Timer("FollowerTimer");
		timer.schedule(task, timeout);
		System.err.println("A FollowerTimer is registered. currentNode: " + currentNode + ", term: " + currentTerm); // for
																														// debug

	}

	/* Leader actions */
	private void leaderActions() {
		System.err.println("This is leader actions. currentNode: " + currentNode + ", term: " + currentTerm);
		// reset the timer for just sending heartbeats
		requestVoteTimer.cancel();
		if (timer != null) {
			timer.cancel();
		}
		timer = new Timer("LeaderTimer");

		if (!isCrashed) {
			sendHeartbeat();
		}

	}

	/* send heartbeats to all other nodes to maintain leadership authority */
	private void sendHeartbeat() {
		System.err.println("Sending heartbeats to all nodes. currentNode: " + currentNode + ", term: " + currentTerm); // debug
		task = new TimerTask() {
			public void run() {
				System.out.println(
						"Task performed on: " + new Date() + "\nThread's name: " + Thread.currentThread().getName()); // for
																														// debug
				if (currentStatus == Status.LEADER && !isCrashed) {
					sendHeartbeat();
				}
			}
		};
		timeout = getRandomTimeout(HEARTBEAT_DURATION); // for debug
		System.err.println("A heartbeat is registered. currentNode: " + currentNode + ", term: " + currentTerm); // for
																													// debug
		timer.schedule(task, timeout);

		// use RPC to send heartbeats via appendEntries()
		try {
			System.err.println("Sending heartbeat... currentNode: " + currentNode + ", term: " + currentTerm); // for
																												// debug
			for (String server : serversList) {
				XmlRpcClient client = new XmlRpcClient("http://" + server + "/RPC2");
				Vector<Object> params = new Vector<>();
				params.add(currentNode); // sender
				params.add(currentTerm); // senderTerm
				params.add(""); // updateInfo
				client.executeAsync("surfstore.appendEntries", params, new AppendEntriesCallback());
			}
		} catch (Exception exception) {
			System.err.println("Exception on StatusManager.sendHeartbeat() is found: ");
			System.err.println(exception);
		}
	}

	/*
	 * Replicate log entries; also serve as a heartbeat mechanism. If the server is
	 * crashed, it should return an “isCrashed” error; procedure has no effect if
	 * server is crashed
	 */
	public boolean appendEntries(String sender, int senderTerm, String updateInfo) {
		System.out.println("appendEntries() of " + currentNode + " is called by the leader. currentNode: " + currentNode
				+ ", term: " + currentTerm); // debug
		// reject this request
		if (isCrashed || senderTerm < currentTerm) {
			return false;
		}

		// if this entry is valid to append, this server must be a follower
		currentStatus = Status.FOLLOWER;
		currentTerm = senderTerm;
		currentLeader = sender;
		votedFor = null; // reset for next election
		// reset timer and task
		registerFollowerTimeout();
		return true;
	}

	// Returns the server's FileInfoMap after communicating with the majority of
	// nodes
	public Hashtable<String, Vector<Object>> getfileinfomap() {
		System.err.println("getfileinfomap() called."); // debug
		return metadataService.getfileinfomap();
	}

	// Update's the given entry in the fileinfomap after communicating with the
	// majority of nodes
	public boolean updatefile(String filename, int version, Vector<String> hashlist) {
		// System.err.println("Updating file: " + filename + "-v" + version); //debug

		boolean isUpdateAccepted = metadataService.updatefile(filename, version, hashlist);
		if (isUpdateAccepted) {
			System.err.println("Successfully update file ino: " + filename + "-v" + version);
		} else {
			System.err.println(
					"Failed to update file ino: " + filename + ", since version " + version + "is out-of-date.");
		}

		return isUpdateAccepted;
	}

	// Queries whether this metadata store is a leader
	// Note that this call should work even when the server is "crashed"
	public boolean isLeader() {
		boolean res = currentStatus == Status.LEADER;
		System.err.println("IsLeader(): " + res); // for debug
		return res;
	}

	// "Crashes" this metadata store
	// Until Restore() is called, the server should reply to all RPCs
	// with an error (unless indicated otherwise), and shouldn't send
	// RPCs to other servers
	public boolean crash() {
		// do nothing if it is already crashed
		if (isCrashed) {
			return true;
		}
		// set the current node as crashed state
		isCrashed = true; 
		// cancel all scheduled task if this server is crashed
		if (timer != null) {
			timer.cancel(); 
		}
		requestVoteTimer.cancel(); 
		// reset
		currentStatus = Status.FOLLOWER;
		votedFor = null;
		System.err.println("Crash(): " + isCrashed);
		return isCrashed;
	}

	// "Restores" this metadata store, allowing it to start responding
	// to and sending RPCs to other nodes
	public boolean restore() {
		// do nothing if it is not crashed
		if (!isCrashed) {
			return true;
		}
		isCrashed = false; // restore the current node by changing crashed state to be uncrashed
		run(); // rerun as a follower
		System.err.println("Restore():" + !isCrashed);
		return !isCrashed;
	}

	// "IsCrashed" returns the status of this metadata node (crashed or not)
	// This method should always work, even when the node is crashed
	public boolean isCrashed() {
		System.err.println("IsCrashed(): " + isCrashed);
		return isCrashed;
	}

	// /* Return the version of the given file, even when the server is crashed */
	// public int tester_getversion(String filename) {
	// Hashtable<String, Vector<Object>> map = getfileinfomap();
	// int version = (int) map.get(filename).get(0);
	// System.out.println("Getting version " + version);
	// return version;
	// }

	/* get a random number determined by the timeout duration */
	private int getRandomTimeout(int duration) {
		int res = (int) ((random.nextDouble() + 1) * duration);
		System.err.println("random timetout: " + res);
		return res;
	}
}