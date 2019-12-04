import java.util.*;
import org.apache.xmlrpc.*;
import java.io.*;
import java.nio.file.*;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;

public class SyncHandler {
    private Hashtable<String, Vector<Object>> localInfoMap; // record the local file info at the client side
    private Hashtable<String, Vector<Object>> remoteInfoMap; // record the remote file info at the server side
    private int blockSize; // the block size of bytes specify by the client
    private String baseDir; // the base directory specify by the client
    private XmlRpcClient client;
    private LocalInfoHandler localInfoHandler;
    private SyncService syncService;

    /* contructor */
    public SyncHandler(XmlRpcClient client, String baseDir, int blockSize) {
        // initialize to avoid null pointer exception
        this.localInfoMap = new Hashtable<String, Vector<Object>>();
        this.remoteInfoMap = new Hashtable<String, Vector<Object>>();

        this.blockSize = blockSize;
        this.baseDir = baseDir;
        this.client = client;
        this.localInfoHandler = new LocalInfoHandler(blockSize);
        this.syncService = new SyncService(client);
    }

    /* handle the synchronization of all files in baseDir */
    public void handle() {
        try {
            baseDir = System.getProperty("user.dir") + baseDir; // get base dir (for local test, delete this line for gradescope)
            
            File directory = new File(baseDir);
            System.err.println("debug: " + baseDir); // debug
            // create base dir if not exists
            if (!directory.exists()) {
                System.err.println("Creating directory: " + baseDir);
                directory.mkdirs();
            }

            // debug
            if (!directory.exists()) {
                System.err.println("Failed to create this directory: " + baseDir);
                System.exit(-1);
            }

            File indexFile = new File(directory + "/index.txt"); // get the index file

            // initialize localInfoMap with info in index.txt
            localInfoHandler.makeLocalInfoMap(localInfoMap, indexFile);
            System.err.println("localInfoMap has been generated."); // debug

            // update localInfoMap by actual files in base dir
            int[] count = localInfoHandler.updateLocalInfoMap(localInfoMap, directory);
            System.err.println("localInfoMap has been updated."); // debug
            System.err.println("Since last sync:");
            if (count[0] == 0 && count[1] == 0 && count[2] == 0) {
                System.err.println(" |-- No changes in local files;");
            }
            if (count[0] > 0) {
                System.err.println(" |-- " + count[0] + " local file(s) has been added;");
            }
            if (count[1] > 0) {
                System.err.println(" |-- " + count[1] + " local file(s) has been modified;");
            }
            if (count[2] > 0) {
                System.err.println(" |-- " + count[2] + " local file(s) has been deleted;");
            }
            System.err.println();

            // sync files in base dir at local with those in the server
            syncFiles(directory);
            System.err.println();

            // generate updated index.txt based on localInfoMap
            localInfoHandler.generateIndexTxt(indexFile, localInfoMap);
            System.err.println("index.txt has been updated.");
            System.err.println();

            // debug
            if (!indexFile.exists()) {
                System.err.println(indexFile.getAbsolutePath() + "does not exist.");
            } else {
                System.err.println("reading " + indexFile.getAbsolutePath());
                Scanner scanner = new Scanner(indexFile);
                while (scanner.hasNextLine()) {
                    System.err.println(scanner.nextLine());
                }
                scanner.close();
            }
            System.err.println();

        } catch (Exception exception) {
            // throw an exception
            System.err.println("Exception on SyncHandler.handle() is found: " + exception);
        }
    }

    /* sync files in base dir at local with those in the server */
    private void syncFiles(File directory) {
        try {
            Vector<Object> params = new Vector<>();

            Set<String> localFilesSet = localInfoMap.keySet(); // get the static file sets at local

            remoteInfoMap = (Hashtable<String, Vector<Object>>) client.execute("surfstore.getfileinfomap", params);
            Set<String> remoteFilesSet = remoteInfoMap.keySet(); // get the static file sets at remote

            // sync existed file to the server
            for (String file : localFilesSet) {

                if (!remoteFilesSet.contains(file)
                        || ((int) localInfoMap.get(file).get(0) > (int) remoteInfoMap.get(file).get(0))) {
                    // new file at local, or latest version of the file, first push data to the
                    // server

                    // System.err.println("Tring to upload \"" + file + "\"..."); // debug

                    Vector<String> hashList = (Vector<String>) localInfoMap.get(file).get(1);
                    if (!hashList.get(0).equals("0")) {
                        // upload existed files to the server, and skip if it is deleted
                        syncService.upload(file, directory, blockSize);
                        System.err.println("> Upload data of \"" + file + "\" successfully.");
                    } else {
                        System.err.println("> \"" + file + "\" has been deleted locally.");
                    }

                    // trying to update local info to the server
                    // System.err.println("Tring to update info of \"" + file + "\"..."); // debug
                    boolean isUpdateAccepted = syncService.updateRemoteInfo(file, localInfoMap);

                    if (!isUpdateAccepted) {
                        // if fail to update the file on the server with the one at local, be consistent
                        // with the server
                        System.err.println("  Server rejected to update info of \"" + file
                                + "\". Downloading the latest version from the server.");

                        // re-download the latest remoteInfoMap and update remoteFilesSet
                        remoteInfoMap = (Hashtable<String, Vector<Object>>) client.execute("surfstore.getfileinfomap",
                                params);
                        remoteFilesSet = remoteInfoMap.keySet(); // get the static file sets at remote

                        updateFromRemote(file, directory);
                    } else {
                        System.err.println("  Update \"" + file + "\" info on the server successfully.");
                    }

                } else if ((int) localInfoMap.get(file).get(0) == (int) remoteInfoMap.get(file).get(0)) {
                    // same version
                    Vector<String> localHashList = (Vector<String>) localInfoMap.get(file).get(1);
                    Vector<String> remoteHashList = (Vector<String>) remoteInfoMap.get(file).get(1);
                    if (!localHashList.equals(remoteHashList)) {
                        System.err.println("> Conflict found in \"" + file
                                + "\". Downloading the latest version from the server.");
                        updateFromRemote(file, directory);
                    } else {
                        System.err.println("> \"" + file + "\" is already the latest version.");
                    }
                } else if ((int) localInfoMap.get(file).get(0) < (int) remoteInfoMap.get(file).get(0)) {
                    // older version, simply update/replace it by the version on the server
                    System.err.println("> Downloading the latest version of \"" + file + "\" from the server.");
                    updateFromRemote(file, directory);
                }
            }

            // download new files from the server
            Set<String> newRemoteFilesSet = new HashSet<>(remoteFilesSet);
            newRemoteFilesSet.removeAll(localFilesSet);
            // System.err.println("newRemoteFilesSet: " + newRemoteFilesSet); // debug
            for (String newFile : newRemoteFilesSet) {
                System.err.println("> Downloading new file \"" + newFile + "\" from the server.");
                updateFromRemote(newFile, directory);
            }


        } catch (Exception e) {
            System.err.println("Exception on SyncHandler.syncFiles() is found: " + e);
        }
    }

    /*
     * Download data from the server, and update file info according to that on the
     * server.
     */
    private void updateFromRemote(String fileName, File directory) {
        syncService.download(fileName, directory, remoteInfoMap);
        // System.err.println("downloaded file " + fileName); // debug
        localInfoMap.put(fileName, remoteInfoMap.get(fileName));
        // System.err.println("updated local info of file " + fileName); // debug
    }
}