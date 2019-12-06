import java.util.*;

/** This class is for handling metadata of files and blocks */
public class MetadataService {
    private Hashtable<String, Vector<Object>> fileInfoMap; // store key-value pairs of filename->Vector, which
                                                           // contains version and hash list (another Vector)

    // /* constructor */
    // public MetadataService(Hashtable<String, Vector<Object>> fileInfoMap) {
    // this.fileInfoMap = fileInfoMap;

    // }
    /* constructor */
    public MetadataService() {
        this.fileInfoMap = new Hashtable<>();
    }

    /* Returns the Hashtable of fileInfoMap */
    public Hashtable<String, Vector<Object>> getfileinfomap() {
        return fileInfoMap;
    }

    /* Update's the given entry in the fileinfomap */
    public boolean updatefile(String filename, int newVersion, Vector<String> hashlist) {
        // check if the server will refuse to update
        if (fileInfoMap.containsKey(filename)) {
            Vector<Object> info = fileInfoMap.get(filename);
            int oldVersion = (int) info.get(0);
            if (oldVersion >= newVersion) {
                // the server will refuse to update
                return false;
            }
        }

        // construct info vector
        Vector<Object> updatedInfo = new Vector<>();
        updatedInfo.add(newVersion);
        updatedInfo.add(hashlist);
        // update fileInfoMap
        fileInfoMap.put(filename, updatedInfo);

        return true;
    }

    /* Return the current version of a given file, 0 if it does not exist */
    public int getFileVersion(String filename) {
        return fileInfoMap.containsKey(filename) ? (int) fileInfoMap.get(filename).get(0) : 0;
    }
}