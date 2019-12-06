import org.apache.xmlrpc.*;
import java.util.*;
import java.net.URL;

class AppendEntriesCallback implements AsyncCallback {
    private Hashtable<String, Integer> nextIndex; // for each server, index of the next log entry to send to that
                                                  // server
    private Hashtable<String, Integer> matchIndex; // for each server, index of log entry that matches the ones of
                                                   // the leader
    private String leader; // leader server that called appendEntries()
    private String follower; // follower that responded appendEntries()
    private int leaderNextIndex; // leader's next index of the last entry in logs
    private HashSet<String> crashedServersSet;

    /* constructor */
    public AppendEntriesCallback(Hashtable<String, Integer> nextIndex, Hashtable<String, Integer> matchIndex,
            String leader, String follower, int leaderNextIndex, HashSet<String> crashedServersSet) {
        this.nextIndex = nextIndex;
        this.matchIndex = matchIndex;
        this.leader = leader;
        this.follower = follower;
        this.leaderNextIndex = leaderNextIndex;
        this.crashedServersSet = crashedServersSet;
    }

    @Override
    public void handleResult(Object o, URL url, String method) {
        try {
            boolean isAccepted = (boolean) o;
            XmlRpcClient client = new XmlRpcClient("http://" + follower + "/RPC2");
            Vector<Object> params = new Vector<>();
			boolean isFollowerCrashed = (boolean) client.execute("surfstore.isCrashed", params);

            if (isAccepted) {
                /*
                 * the entries sent to the follower are accepted, thus the follower has the same
                 * logs as the leader
                 */
                // System.err.println("Accepted new entries started from index " + nextIndex.get(follower) + " from leader "
                //         + leader + " to follower " + follower); // debug
    
                if (nextIndex.get(follower) != leaderNextIndex) {
                    System.err.println("Follower " +follower+ " accepted new entries started from index " + nextIndex.get(follower) + " from leader "
                        + leader); // debug
                }
                nextIndex.put(follower, leaderNextIndex);
                matchIndex.put(follower, leaderNextIndex - 1);
    
                if (crashedServersSet.contains(follower)) {
                    System.err.print(follower + " has restored."); // debug
                    crashedServersSet.remove(follower);
                    // System.err.print("In callback, crashedServersSet.size()= " + crashedServersSet.size()); // debug

                }
    
                // System.err.println("leaderNextIndex: " + leaderNextIndex + ", nextIndex.get(follower): "
                //         + nextIndex.get(follower) + ", matchIndex.get(follower): " + matchIndex.get(follower)); // debug
            } else if (!isFollowerCrashed) {
                /* the entries are not accepted */
                // System.err.print("Rejected new entries started from index " + nextIndex.get(follower) + " from leader "
                //         + leader + " to follower " + follower); // debug
                nextIndex.put(follower, nextIndex.get(follower) - 1); 
                if (crashedServersSet.contains(follower)) {
                    System.err.println(follower + " has restored."); // debug
                    crashedServersSet.remove(follower);
                    // System.err.println("In callback, crashedServersSet.size()= " + crashedServersSet.size()); // debug
                }
                 
            } else {
                System.err.println(follower + " has crashed."); // debug
                crashedServersSet.add(follower);
                // System.err.println("In callback, crashedServersSet.size()= " + crashedServersSet.size()); // debug
            }


        } catch (Exception e) {
            System.err.println("Exception found in AppendEntriesCallback.handleResult(): " + e);
        }
        
    }

    @Override
    public void handleError(Exception e, URL url, String method) {
        System.err.println("Exception found in AppendEntriesCallback: " + e);
    }
}