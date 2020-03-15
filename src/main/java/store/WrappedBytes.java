package store;

import java.util.Arrays;

public class WrappedBytes implements Comparable<WrappedBytes>{
    public final byte [] bytes;
    public WrappedBytes(byte bs[]) {
        bytes = bs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WrappedBytes that = (WrappedBytes) o;
        return Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }


    @Override
    public int compareTo(WrappedBytes o) {
        return comparing(bytes, o.bytes);
    }

    public static final int comparing(byte [] a, byte [] b) {
        int min = Math.min(a.length, b.length);
        int i = 0;
        for (i = 0; i < min; i++) {
            if (a[i] > b[i]) {
                return 1;
            } else if (a[i] < b[i]) {
                return -1;
            }
        }
        if (i == a.length && i == b.length) {
            return 0;
        }
        return a.length - b.length;
    }

    @Override
    public String toString() {
        return new String(bytes);
    }
}
