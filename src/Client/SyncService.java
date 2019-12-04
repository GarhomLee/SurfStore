import java.util.*;
import org.apache.xmlrpc.*;
import java.io.*;
import java.nio.file.*;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;

class SyncService {
    private XmlRpcClient client;
    private Vector<Object> params;

    public SyncService(XmlRpcClient client) {
        this.client = client;
        this.params = new Vector<Object>();
    }

    /*
     * Download file from the server. Note that the file might be deleted on the
     * server, which will have hash value "0".
     */
    public void download(String fileName, File directory, Hashtable<String, Vector<Object>> remoteInfoMap) {
        try {
            File file = new File(directory.getAbsolutePath() + "/" + fileName);

            Vector<String> remoteHashList = (Vector<String>) remoteInfoMap.get(fileName).get(1); // get the hash list

            // the file has been deleted on the server
            if (remoteHashList.get(0).equals("0")) {
                System.err.println("  File \"" + file + "\" has been deleted from the server.");
                if (file.exists()) {
                    // delete the local copy of this file in order to sync with the server
                    System.err.println("  File \"" + file + "\" is now deleted locally.");
                    file.delete();
                }
                return;
            }

            // create the file if not exists locally but on the server
            if (!file.exists()) {
                System.err.println("  Creating new file: " + file.getAbsolutePath());
                file.createNewFile();
            }

            FileOutputStream outputStream = new FileOutputStream(file);
            for (String hashValue : remoteHashList) {
                params.clear(); // must clear Vector before using it
                params.add(hashValue);
                byte[] buffer = (byte[]) client.execute("surfstore.getblock", params);
                outputStream.write(buffer);
            }

            outputStream.close();

        } catch (FileNotFoundException e) {
            System.err.println("SyncService.download(): " + e);
        } catch (IOException e) {
            System.err.println("SyncService.download(): " + e);
        } catch (XmlRpcException e) {
            System.err.println("SyncService.download(): " + e);
        }

    }

    /* Upload file from the server */
    public void upload(String fileName, File directory, int blockSize) {
        try {
            File file = new File(directory.getAbsolutePath() + "/" + fileName);
            byte[] buffer = new byte[blockSize];
            FileInputStream inputStream = new FileInputStream(file);
            int readBytes = 0, totalBytes = 0;

            // use RPC to update blocks of data to the server
            while ((readBytes = inputStream.read(buffer)) != -1) {
                totalBytes += readBytes;

                byte[] readIn = new byte[readBytes]; // byte[] of actual read-in bytes
                System.arraycopy(buffer, 0, readIn, 0, readBytes);

                params.clear(); // must clear Vector before using it
                params.add(readIn);

                boolean isPut = (boolean) client.execute("surfstore.putblock", params);

                if (!isPut) {
                    System.err.println("  Failed to upload data to the server: null byte array.");
                    System.exit(1);
                }
            }

            if (totalBytes == 0) {
                // reading an empty file
                params.clear(); // must clear Vector before using it
                params.add(new byte[0]);

                boolean isPut = (boolean) client.execute("surfstore.putblock", params);

                if (!isPut) {
                    System.err.println("  Failed to upload data to the server: null byte array.");
                    System.exit(1);
                }
            }

            inputStream.close();

        } catch (FileNotFoundException e) {
            System.err.println("SyncService.upload(): " + e);
        } catch (IOException e) {
            System.err.println("SyncService.upload(): " + e);
        } catch (XmlRpcException e) {
            System.err.println("SyncService.upload(): " + e);
        }
    }

    /* Make an attempt to update remote info with local info */
    public boolean updateRemoteInfo(String fileName, Hashtable<String, Vector<Object>> localInfoMap) {
        try {
            params.clear(); // must clear Vector before using it
            params.add(fileName);
            params.add(localInfoMap.get(fileName).get(0));
            params.add(localInfoMap.get(fileName).get(1));

            return (boolean) client.execute("surfstore.updatefile", params);
        } catch (Exception e) {
            System.err.println("SyncService.updateRemoteInfo(): " + e);
        }

        return false;
    }
}