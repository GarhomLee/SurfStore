import java.util.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;

/** This class is for handling all operations about blocks */
public class BlockService {
    private Hashtable<String, byte[]> blockMap; // store key-value pairs of hash->byte blocks

    // /* constructor */
    // public BlockService(Hashtable<String, byte[]> blockMap) {
    //     this.blockMap = blockMap;
    // }
    /* constructor */
    public BlockService() {
        this.blockMap = new Hashtable<>();
    }

    /*
     * Associate bytes block with a specific hash value. Will be called when the
     * client tries to upload files.
     */
    public boolean putblock(byte[] blockData) {
        // corner case
        if (blockData == null) {
            return false;
        }

        String hashvalue = hash(blockData);
        blockMap.put(hashvalue, blockData);
        return true;
    }

    /*
     * Given a hash value, return the associated block. Will be called when the
     * client tries to download files.
     */
    public byte[] getblock(String hashvalue) {
        // if this hash value is not in the blockMap, return byte array with length 0
        return blockMap.containsKey(hashvalue) ? blockMap.get(hashvalue) : new byte[0];
    }

    /* Determine which of the provided blocks are on this server */
    public Set<String> hasblocks(Vector<String> hashlist) {
        Set<String> commonList = new HashSet<>(hashlist);
        commonList.retainAll(blockMap.keySet()); // get the intersection

        return commonList;
    }

    /* get the hash value of the given byte array */
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
            System.err.println("Exception found in BlockService.hash(): " + e);
        }
        return hashvalue;
    }

}