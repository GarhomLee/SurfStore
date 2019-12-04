import java.util.*;
import org.apache.xmlrpc.*;
import java.io.*;
import java.nio.file.*;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;

public class LocalInfoHandler {
    private int blockSize; // the block size of bytes specify by the client

    /* contructor */
    public LocalInfoHandler(int blockSize) {
        this.blockSize = blockSize;
    }

    /*
     * update localInfoMap based on current files in base dir, and return statistics
     * about changes
     */
    public int[] updateLocalInfoMap(Hashtable<String, Vector<Object>> localInfoMap, File directory) {

        Set<String> indexFilesSet = localInfoMap.keySet(); // record files in index.txt

        Set<String> currentFilesSet = new HashSet<>(Arrays.asList(directory.list())); // record current files in base
                                                                                      // dir
        currentFilesSet.remove("index.txt"); // remove index.txt itself

        Set<String> newFilesSet = new HashSet<>(currentFilesSet); // record files that has been created since last sync
        newFilesSet.removeAll(indexFilesSet);

        int addCount = 0, changeCount = 0, deleteCount = 0; // statistics of file changes

        // handle deleted files and remained files
        for (String fileName : indexFilesSet) {
            if (!currentFilesSet.contains(fileName)) {
                Vector<String> hashList = (Vector<String>) localInfoMap.get(fileName).get(1);

                // skip if the deletion info has been recorded in localInfoMap
                if (hashList.get(0).equals("0"))
                    continue;

                // This file has been deleted since last sync
                int newVersion = (int) localInfoMap.get(fileName).get(0) + 1;
                // update and set hash list with only "0"
                Vector<String> newHashList = new Vector<>();
                newHashList.add("0");

                updateSingleEntry(localInfoMap, fileName, newVersion, newHashList);
                deleteCount++; // update count for deleted files

            } else if (isChanged(directory.getAbsolutePath() + "/" + fileName,
                    (Vector<String>) localInfoMap.get(fileName).get(1))) {
                // The file has been changed since last sync according to results of hash
                // comparison

                int newVersion = (int) localInfoMap.get(fileName).get(0) + 1;
                Vector<String> newHashList = getHashList(directory.getAbsolutePath() + "/" + fileName);
                Vector<String> oldHashList = (Vector<String>) localInfoMap.get(fileName).get(1);
                if (oldHashList.get(0).equals("0")) {
                    // the file was deleted and now it is re-added
                    addCount++; // update count for added files
                } else {
                    // the file is modified
                    changeCount++; // update count for changed files
                }
                updateSingleEntry(localInfoMap, fileName, newVersion, newHashList);
            }

        }

        // handle new files
        for (String fileName : newFilesSet) {
            // set the version of new file as 1
            int newVersion = 1;
            Vector<String> newHashList = getHashList(directory.getAbsolutePath() + "/" + fileName);

            updateSingleEntry(localInfoMap, fileName, newVersion, newHashList);
            addCount++; // update count for added files
        }

        return new int[] { addCount, changeCount, deleteCount };
    }

    /* update single entry of key-value pair in localInfoMap */
    public void updateSingleEntry(Hashtable<String, Vector<Object>> localInfoMap, String fileName, int version,
            Vector<String> hashList) {
        Vector<Object> newInfo = new Vector<>();
        // update version
        newInfo.add(version);
        // update hash list
        newInfo.add(hashList);
        // update localInfoMap
        localInfoMap.put(fileName, newInfo);
    }

    /* generate index.txt based on localInfoMap */
    public void generateIndexTxt(File indexFile, Hashtable<String, Vector<Object>> localInfoMap) {
        // write the content from localInfoMap to index.txt
        try {
            FileWriter fileWriter = new FileWriter(indexFile.getAbsolutePath());

            for (String fileName : localInfoMap.keySet()) {
                StringBuilder sb = new StringBuilder(fileName + " "); // construct each line of file info
                Vector<Object> info = localInfoMap.get(fileName);
                // append version
                int version = (int) info.get(0);
                sb.append(version).append(" ");
                // append hash list
                Vector<String> hashList = (Vector<String>) info.get(1);
                for (String hashValue : hashList) {
                    sb.append(hashValue).append(" ");
                }

                sb.setCharAt(sb.length() - 1, '\n'); // replace last space with new line character

                fileWriter.write(sb.toString()); // write this line
            }

            fileWriter.close();

        } catch (IOException e) {
            // exception handling
            System.err.println("generateIndexTxt() in LocalInfoHandler: " + e);
        }
    }

    /* create localInfoMap based on indexFile */
    public void makeLocalInfoMap(Hashtable<String, Vector<Object>> localInfoMap, File indexFile) {
        try {
            // create index.txt if not exists
            if (!indexFile.exists()) {
                System.err.println("Creating new local index: " + indexFile.getAbsolutePath());
                indexFile.createNewFile();
            }

            BufferedReader bufferedReader = new BufferedReader(new FileReader(indexFile.getAbsolutePath()));
            String line = bufferedReader.readLine(); // read the first line

            System.err.println("Reading from index.txt...");
            while (line != null) {
                String[] split = line.split("\\s+"); // split each line in index.txt by space

                String fileName = split[0]; // get file name

                Vector<Object> info = new Vector<>(); // include version and a hash list
                int version = Integer.parseInt(split[1]); // get version
                info.add(version); // add version

                Vector<String> hashList = new Vector<>();
                for (int i = 2; i < split.length; i++) {
                    hashList.add(split[i]);
                }
                info.add(hashList); // add hash list

                localInfoMap.putIfAbsent(fileName, info);

                // read the next line
                line = bufferedReader.readLine();
            }

            bufferedReader.close();

        } catch (FileNotFoundException e) {
            System.err.println("makeLocalInfoMap() in LocalInfoHandler: " + e);
        } catch (IOException e) {
            System.err.println("makeLocalInfoMap() in LocalInfoHandler: " + e);
        }
    }

    /* compare and determin if the file (absolute path) has been changed */
    private boolean isChanged(String filePath, Vector<String> hashList) {
        return !getHashList(filePath).equals(hashList);
    }

    /* get the hash list of the given file (absolute path) */
    private Vector<String> getHashList(String filePath) {
        Vector<String> hashList = new Vector<String>();

        try {
            File file = new File(filePath);
            byte[] buffer = new byte[blockSize];
            FileInputStream inputStream = new FileInputStream(file);
            int readBytes = 0, totalBytes = 0;

            // hash the file by blockSize
            while ((readBytes = inputStream.read(buffer)) != -1) {
                totalBytes += readBytes;
                byte[] readIn = new byte[readBytes]; // byte[] of actual read-in bytes
                System.arraycopy(buffer, 0, readIn, 0, readBytes);

                String hashValue = hash(readIn);
                hashList.add(hashValue);
            }

            if (totalBytes == 0) {
                // reading an empty file
                hashList.add(hash(new byte[0]));
            }

            inputStream.close();

        } catch (FileNotFoundException e) {
            System.err.println("getHashList() in LocalInfoHandler: " + e);
        } catch (IOException e) {
            System.err.println("getHashList() in LocalInfoHandler: " + e);
        }

        return hashList;
    }

    /* hash blockData into String representation */
    private String hash(byte[] blockData) {
        String hashvalue = "";
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(blockData);
            BigInteger number = new BigInteger(1, hash);
            StringBuilder hexString = new StringBuilder(number.toString(16));
            while (hexString.length() < 32) {
                hexString.insert(0, '0');
            }
            hashvalue = hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            System.err.println("Hash block data: " + e);
        }
        return hashvalue;
    }
}