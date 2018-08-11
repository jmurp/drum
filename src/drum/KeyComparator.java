package drum;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Comparator;

public class KeyComparator implements Comparator<byte[]>, Serializable {
    public int compare(byte[] b1, byte[] b2) {

        return new BigInteger(b1).compareTo(new BigInteger(b2));
    }
}
