import org.apache.xmlrpc.*;
import java.util.*;
import java.net.URL;

class RequestVoteCallback implements AsyncCallback {
    private HashSet<String> votedSet;  // record servers that has voted, either "for" or "against"
	private HashSet<String> votedForSet;  // record servers that has voted "for"

    public RequestVoteCallback(HashSet<String> votedSet, HashSet<String> votedForSet) {
        this.votedSet = votedSet;
        this.votedForSet = votedForSet;
        // this.currentTerm = currentTerm;
    }

    @Override
    public void handleResult(Object o, URL url, String method) {
        Vector<Object> voteResult = (Vector<Object>) o;
        boolean isVoted = (boolean) voteResult.get(0);
        String voteFrom = (String) voteResult.get(1);
        // int votedTerm = (int) voteResult.get(2);
        votedSet.add(voteFrom);
        if (isVoted) {
            System.err.println("receive vote from: " + voteFrom);
            votedForSet.add(voteFrom);
        } else {
            // debug
            System.err.println("vote denied by: " + voteFrom);
        }
    }

    @Override
    public void handleError(Exception e, URL url, String method) {
        System.err.println("Exception found in RequestVoteCallBack: " + e);
    }
}