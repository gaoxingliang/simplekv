package store;

public interface KVStore {
    int MAX_KEY_LEN = 20;
    int MAX_VALUE_LEN = 100;

    void open() throws Exception;
    void close() throws Exception;
    void put(byte[] key, byte[] value) throws Exception;
    byte [] get(byte [] key) throws Exception;

    Metrics metrics();
}
