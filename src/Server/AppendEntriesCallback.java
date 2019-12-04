import org.apache.xmlrpc.*;
import java.util.*;
import java.net.URL;

class AppendEntriesCallback implements AsyncCallback {

    public AppendEntriesCallback() {
    }

    public void handleResult(Object o, URL url, String method) {
        
    }

    public void handleError(Exception e, URL url, String method) {
        System.err.println("Exception found in AppendEntriesCallback: " + e);
    }
}