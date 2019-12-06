import org.apache.xmlrpc.*;
import java.util.*;
import java.net.URL;

class RequestVoteCallback implements AsyncCallback {
    private HashSet<String> votedSet; // record servers that has voted, either "for" or "against"
    private HashSet<String> votedForSet; // record servers that has voted "for"
    private String requestor; // server that requested this vote
    private String voter; // server that responded to this vote

    public RequestVoteCallback(HashSet<String> votedSet, HashSet<String> votedForSet, String requestor, String voter) {
        this.votedSet = votedSet;
        this.votedForSet = votedForSet;
        this.requestor = requestor;
        this.voter = voter;
    }

    @Override
    public void handleResult(Object o, URL url, String method) {
        boolean isVoted = (boolean) o;
        votedSet.add(voter);
        if (isVoted) {
            System.err.println(requestor + ": receive vote from: " + voter);
            votedForSet.add(voter);
        } else {
            System.err.println(requestor + ": vote denied by: " + voter); // debug
        }
    }

    @Override
    public void handleError(Exception e, URL url, String method) {
        System.err.println("Exception found in RequestVoteCallBack: " + e);
        System.err.println("method in exception: " + method);
    }
}