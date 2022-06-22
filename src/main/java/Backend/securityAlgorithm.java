package Backend;
//Author: @Smit_Thakkar
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class securityAlgorithm {

    public static String getHash(final String source) throws NoSuchAlgorithmException {
        if (source == null) {
            return null;
        }
        final MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
        return String.format("%064x", new BigInteger(1, messageDigest.digest(source.getBytes(StandardCharsets.UTF_8))));
    }

    public static boolean validateHash(final String source, final String targetHash) throws NoSuchAlgorithmException {
        final String sourceHash = getHash(source);
        if (sourceHash == null || targetHash == null) {
            return false;
        }
        return sourceHash.equals(targetHash);
    }
}