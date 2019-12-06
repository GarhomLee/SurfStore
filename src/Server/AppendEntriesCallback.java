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

    /* constructor */
    public AppendEntriesCallback(Hashtable<String, Integer> nextIndex, Hashtable<String, Integer> matchIndex,
            String leader, String follower, int leaderNextIndex) {
        this.nextIndex = nextIndex;
        this.matchIndex = matchIndex;
        this.leader = leader;
        this.follower = follower;
        this.leaderNextIndex = leaderNextIndex;
    }

    @Override
    public void handleResult(Object o, URL url, String method) {
        boolean isAccepted = (boolean) o;
        if (isAccepted) {
            /*
             * the entries sent to the follower are accepted, thus the follower has the same
             * logs as the leader
             */
            System.err.println("Accepted new entries started from index " + nextIndex.get(follower) + " from leader " + leader
                    + " to follower " + follower); // debug
            nextIndex.put(follower, leaderNextIndex);
            matchIndex.put(follower, leaderNextIndex - 1);
        } else {
            /* the entries are not accepted */
            System.err.print("Rejected new entries started from index " + nextIndex.get(follower) + " from leader " + leader
                    + " to follower " + follower); // debug
            nextIndex.put(follower, nextIndex.get(follower) - 1);
            System.err.print(". Try again from index " + nextIndex.get(follower));
        }
    }

    @Override
    public void handleError(Exception e, URL url, String method) {
        System.err.println("Exception found in AppendEntriesCallback: " + e);
    }
}