import org.apache.xmlrpc.*;
import java.util.*;

/**
 * This class is for managing the different statuses of a server and its
 * corresponding behaviors
 */
class StatusManager {
	// // for local test
	// private int TIMEOUT_DURATION = 5000; // timeout duration in milliseconds
	// private int HEARTBEAT_DURATION = 2500; // heartbeat duration in milliseconds,
	// will not be random
	// for gradescope
	private int TIMEOUT_DURATION = 1500; // timeout duration in milliseconds
	private int HEARTBEAT_DURATION = 500; // heartbeat duration inmilliseconds, will not be random
	// private int HEARTBEAT_DURATION = 100;

	private String currentNode; // "host:port" of current node
	private Vector<String> serversList; // record all servers' "host:port" info, excluding this server
	private Status currentStatus; // status of the server
	private MetadataService metadataService; // handle metadata
	private int currentTerm; // latest term server has seen (initialized to 0 on first boot, increases
								// monotonically)
	private String votedFor; // candidate server that received vote in a given term (or null if none), only
								// valid in election period. Reset null in a new term
	private HashSet<String> votedSet; // record servers that has voted, either "for" or "against"
	private HashSet<String> votedForSet; // record servers that has voted "for"
	private Vector<Vector<Object>> logs; // log entries; each entry contains:
											// (1) the term when entry was received by leader (first index is 0)
											// (2) a state machine (update of a single file)
	private int commitIndex; // index of highest log entry known to be committed (initialized to -1,
								// increases monotonically)
	private int lastApplied; // index of highest log entry applied to state machine (initialized to -1,
								// increases monotonically)
	private Hashtable<String, Integer> nextIndex; // for each server, index of the next log entry to send to that
													// server, initialized to leader last log index + 1
	private Hashtable<String, Integer> matchIndex; // for each server, index of log entry that matches the ones of
													// the leader, initialized to -1

	private String currentLeader; // indicate the current leader in this term
	private int totalNodesNum; // the total number of nodes, including current node itself
	private int majorityNodeNum; // the number of majority in the given cluster
	private Timer timer; // timer for follower timeout and election timeout
	private TimerTask task;
	private Timer requestVoteTimer; // timer for sending next vote request
	private TimerTask requestVoteTask;
	private Timer applyCommitsTimer; // timer for periodically applying commits
	private TimerTask applyCommitsTask;
	private Timer leaderCommitUpdateTimer; // timer for periodically update leader's commit index
	private TimerTask leaderCommitUpdateTask;
	private int timeout; // timeout duration, depending on random number
	private Random random; // introduce random duration for timeout
	private boolean isCrashed; // indicate if this node is in the crashed state

	/* all possible statuses of a node */
	public enum Status {
		LEADER, CANDIDATE, FOLLOWER;
	}

	/* constructor */
	public StatusManager(String currentNode, Vector<String> serversList) {
		this.currentNode = currentNode;
		this.serversList = serversList;
		this.metadataService = new MetadataService();
		this.currentStatus = Status.FOLLOWER;
		this.totalNodesNum = serversList.size() + 1;
		this.majorityNodeNum = totalNodesNum / 2 + 1;
		this.votedSet = new HashSet<>();
		this.votedForSet = new HashSet<>();
		this.currentTerm = 0;
		this.logs = new Vector<>();
		this.commitIndex = -1;
		this.lastApplied = -1;
		this.nextIndex = new Hashtable<>();
		this.matchIndex = new Hashtable<>();
		this.random = new Random();
		this.isCrashed = false;
		this.timer = new Timer("FollowerTimer");
		this.requestVoteTimer = new Timer("RequestVoteTimer");
		this.applyCommitsTimer = new Timer("ApplyCommitsTimer");
		;
		this.leaderCommitUpdateTimer = new Timer("LeaderCommitUpdateTimer");
	}

	/* entrance funciton to start a new server */
	public void run() {
		// begin as a follower
		this.commitIndex = -1;
		this.lastApplied = -1;

		registerFollowerTimeout();
		applyCommits();
	}

	/* start a new term and election */
	private void startElection() {
		// System.err.println("Current term " + currentTerm + " of server " +
		// currentNode + " has timed out.");

		/* cancel all other timers */
		leaderCommitUpdateTimer.cancel();
		requestVoteTimer.cancel();

		/* register a timeout for this election period */
		task = new TimerTask() {
			public void run() {
				// start a new term and election if this election period has timed out
				if (currentStatus == Status.CANDIDATE && !isCrashed) {
					startElection();
				}
			}
		};
		timer.cancel(); // terminates this timer, discarding any currently scheduled tasks.
		timer = new Timer("ElectionTimer");
		// System.err
		// .println("A startElection timer is registered. currentNode: " + currentNode +
		// ", term: " + currentTerm); // for
		// debug
		timer.schedule(task, getRandomTimeout(TIMEOUT_DURATION)); // timeout start

		/* set up for a new election */
		currentTerm += 1;
		currentLeader = null;
		currentStatus = Status.CANDIDATE;
		// System.err.println("Election for new term " + currentTerm + " has started.
		// currentNode: " + currentNode
		// + ", term: " + currentTerm); // debug
		votedSet.clear(); // reset to empty set
		votedSet.add(currentNode); // add itself
		votedForSet.clear();
		votedForSet.add(currentNode);
		votedFor = currentNode; // vote for itself

		/*
		 * request votes from all other nodes if it is still a candidate and not crashed
		 */
		if (currentStatus == Status.CANDIDATE && !isCrashed) {
			sendVoteRequests();
		}
	}

	/* send async requests for election votes */
	private void sendVoteRequests() {
		requestVoteTask = new TimerTask() {
			public void run() {
				// check if it is still a candicate
				if (currentStatus == Status.CANDIDATE) {
					if (votedSet.size() < majorityNodeNum) {
						// not enough votes, resend requestVote()
						// System.err.println("Only " + votedSet.size() + " out of " + totalNodesNum + "
						// respond, with "
						// + votedForSet.size() + " positive votes. Resend requests for votes.
						// currentNode: "
						// + currentNode + ", term: " + currentTerm); // for debug
						if (!isCrashed) {
							sendVoteRequests();
						}
					} else if (votedForSet.size() >= majorityNodeNum) {
						// receive enough votes as well as positive votes, become a leader
						// System.err.println("[" + currentNode + "] is now the leader with " +
						// votedForSet.size()
						// + " votes. Current term is: " + currentTerm);
						if (!isCrashed) {
							setupLeader();
						}
					}
				}
			}
		};

		// System.err.println("A requestVoteTimer is registered. currentNode: " +
		// currentNode + ", term: " + currentTerm); // for
		// debug
		requestVoteTimer.cancel();
		requestVoteTimer = new Timer("RequestVoteTimer");
		requestVoteTimer.schedule(requestVoteTask, HEARTBEAT_DURATION); // timeout start

		// use RPC to request votes from all other nodes
		try {
			// System.err.println("Sending voting request... currentNode: " + currentNode +
			// ", term: " + currentTerm); // for
			// debug
			for (String server : serversList) {
				// skip the servers if it is voted for (not against) this server
				if (votedSet.contains(server))
					continue;

				XmlRpcClient client = new XmlRpcClient("http://" + server + "/RPC2");
				Vector<Object> params = new Vector<>();
				// check if the voter is not crashed and can respond. If not, skip it and ask
				// again in the future
				boolean isVoterCrashed = (boolean) client.execute("surfstore.isCrashed", params);
				if (isVoterCrashed)
					continue;

				params.clear();
				params.add(currentNode); // requestor
				params.add(currentTerm); // requestorTerm
				int requestorLastLogIndex = logs.size() - 1;
				params.add(requestorLastLogIndex); // requestorLastLogIndex
				int requestorLastLogTerm = requestorLastLogIndex < 0 ? 0 : (int) logs.get(requestorLastLogIndex).get(0);
				params.add(requestorLastLogTerm); // requestorLastLogTerm

				client.executeAsync("surfstore.requestVote", params,
						new RequestVoteCallback(votedSet, votedForSet, currentNode, server));
			}
		} catch (Exception exception) {
			// catch XmlRpcException
			System.err.println("Exception on StatusManager.sendVoteRequests() is found: ");
			System.err.println(exception);
		}
	}

	/*
	 * Used to implement leader election. Return: (1) if it grants the vote to the
	 * request; (2) the currnet server. If the server is crashed, it should return
	 * an “isCrashed” error; procedure has no effect if server is crashed
	 */
	public boolean requestVote(String requestor, int requestorTerm, int requestorLastLogIndex,
			int requestorLastLogTerm) {
		// System.err.println(
		// "requestVote() of: " + currentNode + ", requested by: " + requestor + ", for
		// term: " + requestorTerm); // debug
		int lastLogIndex = logs.size() - 1;
		int lastLogTerm = lastLogIndex < 0 ? 0 : (int) logs.get(lastLogIndex).get(0);

		// determine if it should grant vote to the requestor
		boolean isVoted = requestorTerm > currentTerm; // check term
		if (isVoted) {
			// term should update, and change into follower
			currentTerm = requestorTerm;
			currentStatus = Status.FOLLOWER;
			currentLeader = null;
			votedFor = null;
			registerFollowerTimeout();
		}
		// System.err.println("isVoted: if it is valid term comparison: " + isVoted); //
		// debug
		isVoted = isVoted && (lastLogTerm < requestorLastLogTerm
				|| (lastLogTerm == requestorLastLogTerm && lastLogIndex <= requestorLastLogIndex)); // check last log
		// System.err.println("isVoted: if it is valid log comparion : " + isVoted); //
		// debug
		isVoted = isVoted && (votedFor == null || votedFor.equals(requestor)); // check if it has granted vote
		// System.err.println("isVoted: if it is valid voterFor : " + isVoted); // debug

		// for debug
		if (isVoted) {
			// System.err.println("This vote is granted. currentNode: " + currentNode + ",
			// term: " + currentTerm);
			votedFor = requestor; // grant vote to the requestor
		} else {
			// System.err.println("This vote is denied. currentNode: " + currentNode + ",
			// term: " + currentTerm);
		}

		return isVoted;
	}

	/* register follower timeout */
	private void registerFollowerTimeout() {
		// cancel the next request vote schedule if it is already a follower
		leaderCommitUpdateTimer.cancel();
		requestVoteTimer.cancel();

		task = new TimerTask() {
			public void run() {
				// System.out.println(
				// "Task performed on: " + new Date() + "\nThread's name: " +
				// Thread.currentThread().getName()); // for
				// debug
				// start a new term and election if this follower period has timed out
				if (!isCrashed) {
					startElection();
				}

			}
		};
		timer.cancel(); // terminates this timer, discarding any currently scheduled tasks.
		timer = new Timer("FollowerTimer");
		timer.schedule(task, getRandomTimeout(TIMEOUT_DURATION));
		// System.err.println("A FollowerTimer is registered. currentNode: " +
		// currentNode + ", term: " + currentTerm); // debug
	}

	/* set up a leader */
	private void setupLeader() {
		// System.err.println("This is leader actions. currentNode: " + currentNode + ",
		// term: " + currentTerm);
		/* cancel the relevant timers */
		requestVoteTimer.cancel();
		timer.cancel(); // terminates this timer, discarding any currently scheduled tasks.

		/* set up all states for a leader */
		currentStatus = Status.LEADER;
		currentLeader = currentNode;
		/* reset nextIndex map */
		nextIndex.clear();
		for (String server : serversList) {
			nextIndex.put(server, logs.size());
		}
		/* reset matchIndex map */
		matchIndex.clear();
		for (String server : serversList) {
			matchIndex.put(server, -1);
		}

		sendHeartbeat();
		updateLeaderCommitIndex();
	}

	/* send heartbeats to all other nodes to maintain leadership authority */
	private void sendHeartbeat() {
		// System.err.println("Sending heartbeats to all nodes. currentNode: " + currentNode + ", term: " + currentTerm); // debug
		task = new TimerTask() {
			public void run() {
				if (isLeader() && !isCrashed) {
					sendHeartbeat();
				}
			}
		};
		// timeout = getRandomTimeout(HEARTBEAT_DURATION); // for debug
		// System.err.println("A heartbeat is registered. currentNode: " + currentNode +
		// ", term: " + currentTerm); // debug

		/* set timer for just sending heartbeats */
		timer.cancel();
		timer = new Timer("SendHeartBeatTimer");
		timer.schedule(task, HEARTBEAT_DURATION);

		// use RPC to send heartbeats via appendEntries()
		try {
			// System.err.println("Sending heartbeat... currentNode: " + currentNode + ",
			// term: " + currentTerm); // debug

			// public boolean appendEntries(String sender, int senderTerm,
			// Vector<Vector<Object>> newEntries, int prevLogIndex,
			// int prevLogTerm, int leaderCommit)
			for (String server : serversList) {
				XmlRpcClient client = new XmlRpcClient("http://" + server + "/RPC2");
				Vector<Object> params = new Vector<>();

				/*
				 * check if the follower is not crashed and can respond. If not, skip it and ask
				 * again in the future
				 */
				boolean isFollowerCrashed = (boolean) client.execute("surfstore.isCrashed", params);
				if (isFollowerCrashed)
					continue;

				/* marshal all parameters for appendEntries() */
				// System.err.println("heartbeat: follower alive"); // debug
				params.clear();
				params.add(currentNode); // sender
				params.add(currentTerm); // senderTerm
				// System.err.println("heartbeat: currentNode = " + currentNode +
				// ",currentTerm=" + currentTerm); // debug

				int serverNextIndex = nextIndex.get(server);
				// System.err.println("heartbeat: server=" + server + ",serverNextIndex=" +
				// serverNextIndex); // debug

				Vector<Vector<Object>> newEntries = new Vector<>();
				for (int idx = serverNextIndex; idx < logs.size(); idx++) {
					newEntries.add(new Vector<>(logs.get(idx)));
				}
				params.add(newEntries); // newEntries
				// System.err.println("heartbeat: newEntries = " + newEntries.toString()); //
				// debug

				int prevLogIndex = serverNextIndex - 1;
				params.add(prevLogIndex); // prevLogIndex
				// System.err.print("heartbeat: prevLogIndex = " + prevLogIndex + ","); // debug

				// int prevLogTerm = logs.isEmpty() ? 0 : (int) logs.get(prevLogIndex).get(0);
				int prevLogTerm = prevLogIndex < 0 ? 0 : (int) logs.get(prevLogIndex).get(0);
				params.add(prevLogTerm); // prevLogTerm
				// System.err.print("prevLogTerm=" + prevLogTerm + ","); // debug

				params.add(commitIndex); // leaderCommit
				// System.err.println("commitIndex=" + commitIndex); // debug

				client.executeAsync("surfstore.appendEntries", params,
						new AppendEntriesCallback(nextIndex, matchIndex, currentNode, server, logs.size()));
			}
		} catch (Exception exception) {
			// catch XmlRpcException
			System.err.println("Exception on StatusManager.sendHeartbeat() is found: ");
			System.err.println(exception);
		}
	}

	/*
	 * Check if there are majority of servers alive
	 */
	private boolean checkWorkingFollowers() throws CrashedServerException {
		int workingFollowersNum = 1;

		try {
			// HashSet<String> workingFollowersSet = new HashSet<>(); // record alive
			// followers
			// workingFollowersSet.add(currentNode);

			// // synchronously check if there are majority of servers alive
			// while (workingFollowersSet.size() < majorityNodeNum) {
			// for (String server : serversList) {
			// if (this.isCrashed) {
			// // throw exception if crashed and not execute this function
			// throw new CrashedServerException();
			// }
			// // skip if this node has been asked
			// if (workingFollowersSet.contains(server))
			// continue;

			// XmlRpcClient client = new XmlRpcClient("http://" + server + "/RPC2");
			// Vector<Object> params = new Vector<>();
			// // check if the follower is not crashed and can respond
			// boolean isFollowerCrashed = (boolean) client.execute("surfstore.isCrashed",
			// params);
			// if (!isFollowerCrashed) {
			// workingFollowersSet.add(server);
			// }
			// }
			// }

			for (String server : serversList) {
				if (this.isCrashed) {
					// throw exception if crashed and not execute this function
					throw new CrashedServerException();
				}

				XmlRpcClient client = new XmlRpcClient("http://" + server + "/RPC2");
				Vector<Object> params = new Vector<>();
				// check if the follower is not crashed and can respond
				boolean isFollowerCrashed = (boolean) client.execute("surfstore.isCrashed", params);
				if (!isFollowerCrashed) {
					workingFollowersNum += 1;
				}
			}

		} catch (CrashedServerException e) {
			// catch CrashedServerException
			System.err.println("CrashedServerException found in StatusManager.checkWorkingFollowers(): ");
			System.err.println(e);
			throw new CrashedServerException();
		} catch (Exception e) {
			// catch XmlRpcException
			System.err.println("Exception found in StatusManager.checkWorkingFollowers(): ");
			System.err.println(e);
		}

		return workingFollowersNum >= majorityNodeNum; // true if majority are alive
	}

	/**
	 * Replicate log entries; also serve as a heartbeat mechanism. If the server is
	 * crashed, it should return an “isCrashed” error; procedure has no effect if
	 * server is crashed
	 */
	public boolean appendEntries(String sender, int senderTerm, Vector<Vector<Object>> newEntries, int prevLogIndex,
			int prevLogTerm, int leaderCommit) {
		// System.out.println("appendEntries() of " + currentNode + " is called by the
		// leader. currentNode: " + currentNode
		// + ", term: " + currentTerm); // debug

		/*
		 * reject this request if the term is not valid. In this case, the follower
		 * timer will not reset.
		 */
		if (senderTerm < currentTerm) {
			// System.err.println("appendEntries() rejected: wrong term"); // debug
			return false;
		}

		/* if sender's term is valid, this server must be a follower */
		currentStatus = Status.FOLLOWER;
		if (senderTerm > currentTerm) {
			votedFor = null; // reset for new term
		}
		currentTerm = senderTerm;
		currentLeader = sender;
		registerFollowerTimeout(); // reset timer and task
		int lastEntryIndex = logs.size() - 1; // get the last index of log entries

		/*
		 * refuse to append if this server has entries less than what the leader knows
		 */
		if (lastEntryIndex < prevLogIndex) {
			return false;
		}
		/*
		 * refuse to append if this server's entry does not match what the leader knows
		 */
		if (!logs.isEmpty() && ((int) logs.get(prevLogIndex).get(0) != prevLogTerm)) {
			return false;
		}

		/* remove all entries if this server contains more than what the leader knows */
		while (!logs.isEmpty() && lastEntryIndex > prevLogIndex) {
			logs.remove(lastEntryIndex);
			lastEntryIndex -= 1;
		}

		/*
		 * Append any new entries not already in the log. If new entries vector is
		 * empty, it is a heartbeat
		 */
		if (!newEntries.isEmpty()) {
			logs.addAll(new Vector<>(newEntries));
		}
		lastEntryIndex = logs.size() - 1; // update lastEntryIndex
		commitIndex = Math.min(leaderCommit, lastEntryIndex); // update commitIndex

		// System.err.println("lastEntryIndex: " + lastEntryIndex + ",leaderCommit: " +
		// leaderCommit + ",commitIndex: "
		// + commitIndex); // debug

		return true;
	}

	/**
	 * Apply all commits if commitIndex has been updated
	 */
	private void applyCommits() {
		// System.err.println("Attemp to apply commits. currentNode: " + currentNode +
		// ", term: " + currentTerm); // debug
		/* register a applyCommitsTimer for periodically applying commits */
		applyCommitsTask = new TimerTask() {
			public void run() {
				if (!isCrashed) {
					applyCommits();
				}
			}
		};
		applyCommitsTimer.cancel(); // cancel the old timer
		applyCommitsTimer = new Timer("ApplyCommitsTimer");
		applyCommitsTimer.schedule(applyCommitsTask, HEARTBEAT_DURATION);

		/* apply commits when commitIndex is larger than lastApplied */
		while (lastApplied < commitIndex) {
			Vector<Object> newFileInfo = (Vector<Object>) logs.get(lastApplied + 1).get(1); // get the newFileInfo
																							// to apply
			// unmarshal all parameters for updatefile()
			String filename = (String) newFileInfo.get(0);
			int newVersion = (int) newFileInfo.get(1);
			Vector<String> hashlist = (Vector<String>) newFileInfo.get(2);
			// metadataService will handle the update, either approve or reject
			metadataService.updatefile(filename, newVersion, hashlist);
			// update lastApplied
			lastApplied += 1;
		}

		// System.err.println("logs size=" + logs.size() + ",last applied=" + lastApplied + ",commit index=" + commitIndex); // debug
	}

	/**
	 * Update leader's commit index by checking the mode of match index is at least
	 * the majority of all nodes
	 */
	private void updateLeaderCommitIndex() {
		// System.err.println(
		// "Attemp to update leader's commit index. currentNode: " + currentNode + ",
		// term: " + currentTerm); // debug
		/* register a leaderCommitUpdateTimer for periodically applying commits */
		leaderCommitUpdateTask = new TimerTask() {
			public void run() {
				/* check if it is still a functional leader */
				if (!isCrashed && isLeader()) {
					updateLeaderCommitIndex();
				}
			}
		};
		leaderCommitUpdateTimer.cancel(); // cancel the old timer
		leaderCommitUpdateTimer = new Timer("LeaderCommitUpdateTimer");
		leaderCommitUpdateTimer.schedule(leaderCommitUpdateTask, HEARTBEAT_DURATION);

		/* use Boyer-Moore Voting Algorithm */
		int count = 1;
		int leaderLastIndex = this.logs.size() - 1;
		int candidateCommitIndex = leaderLastIndex;
		for (String server : matchIndex.keySet()) {
			int currentMatchIndex = matchIndex.get(server);
			// System.err.println("matchIndex of " + server + " is: " + matchIndex.get(server)); // debug
			if (count == 0) {
				candidateCommitIndex = currentMatchIndex;
			}

			count += candidateCommitIndex == currentMatchIndex ? 1 : -1;
		}

		/* check if the count of match index is at least the majority of all nodes */
		count = candidateCommitIndex == leaderLastIndex ? 1 : 0;
		for (String server : matchIndex.keySet()) {
			int currentMatchIndex = matchIndex.get(server);
			count += candidateCommitIndex == currentMatchIndex ? 1 : 0;
		}
		if (count >= majorityNodeNum) {
			// System.err.println("Majority nodes have match index: " +
			// candidateCommitIndex); // debug
			commitIndex = Math.max(commitIndex, candidateCommitIndex); // update as the bigger index
		}

		// System.err.println("commitIndex now is: " + commitIndex); // debug
	}

	// Returns the server's FileInfoMap after communicating with the majority of
	// nodes
	public Hashtable<String, Vector<Object>> getfileinfomap() throws CrashedServerException {
		// System.err.println("getfileinfomap() in StatusManager is called."); // debug

		try {
			// synchronously check if there are majority of servers alive
			while (!checkWorkingFollowers()) {
				continue;
			}

		} catch (CrashedServerException e) {
			// catch an exception of crashed server
			System.err.println("CrashedServerException in StatusManager.getfileinfomap() is found: ");
			System.err.println(e);
			throw new CrashedServerException();
		}

		// get the result if majority of servers are alive
		return metadataService.getfileinfomap();
	}

	// Update's the given entry in the fileinfomap after communicating with the
	// majority of nodes
	public boolean updatefile(String filename, int version, Vector<String> hashlist) throws CrashedServerException {
		// System.err.println("Updating file: " + filename + "-v" + version); //debug

		boolean isUpdateAccepted = false;
		try {
			isUpdateAccepted = metadataService.updatefile(filename, version, hashlist);
			if (isUpdateAccepted) {
				System.err.println("Update file ino: " + filename + "-v" + version + " will succeed.");
			} else {
				System.err.println(
						"Update file ino: " + filename + " will fail, since version " + version + "is out-of-date.");
			}

			// synchronously check if there are majority of servers alive
			while (!checkWorkingFollowers()) {
				continue;
			}

			// marshal all info of the new file
			Vector<Object> newFileInfo = new Vector<>();
			newFileInfo.add(filename);
			newFileInfo.add(version);
			newFileInfo.add(hashlist);
			// assemble the term and new file info in a new entry
			Vector<Object> entry = new Vector<>();
			entry.add(currentTerm);
			entry.add(newFileInfo);
			// append new entry into the log of the leader
			logs.add(entry);

		} catch (CrashedServerException e) {
			// catch an exception of crashed server
			System.err.println("CrashedServerException in StatusManager.updatefile() is found: ");
			System.err.println(e);
			throw new CrashedServerException();
		}

		// reply the result back to caller if majority of servers are alive
		return isUpdateAccepted;
	}

	// Queries whether this metadata store is a leader
	// Note that this call should work even when the server is "crashed"
	public boolean isLeader() {
		boolean res = this.currentStatus == Status.LEADER;
		// System.err.println("IsLeader(): " + res); // for debug
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
		timer.cancel();
		requestVoteTimer.cancel();
		applyCommitsTimer.cancel();
		leaderCommitUpdateTimer.cancel();
		// reset states
		currentStatus = Status.FOLLOWER;
		votedFor = null;
		// System.err.println("Crash(): " + isCrashed);
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
		// System.err.println("Restore():" + !isCrashed);
		return !isCrashed;
	}

	// "IsCrashed" returns the status of this metadata node (crashed or not)
	// This method should always work, even when the node is crashed
	public boolean isCrashed() {
		// System.err.println("IsCrashed(): " + isCrashed);
		return isCrashed;
	}

	/* get a random number determined by the timeout duration */
	private int getRandomTimeout(int duration) {
		int res = (int) ((random.nextDouble() + 1) * duration);
		// System.err.println("duration: " + duration + "; random timetout: " + res); //
		// debug
		return res;
	}

	/* Return the current version of a given file, 0 if it does not exist */
	public int getFileVersion(String filename) {
		// System.err.println("StatusManager.getFileVersion()");
		return metadataService.getFileVersion(filename);
	}
}