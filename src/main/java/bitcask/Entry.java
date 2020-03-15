package bitcask;

import store.WrappedBytes;

public class Entry {
    public WrappedBytes key;
    public byte[] value;
    public long offset = -1; // not set yet

    public Entry(WrappedBytes key, byte[] value, long offset) {
        this.key = key;
        this.value = value;
        this.offset = offset;
    }

    public Entry(WrappedBytes key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public int writeSize() {
        return key.bytes.length + value.length + 8;
    }
}
