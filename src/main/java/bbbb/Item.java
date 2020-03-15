package bbbb;

public class Item {
    int fileId;
    long offset;// 整个最开始的offset
    int klen, vlen;
    byte[] key;

    public Item(int fileId, long offset, int klen, int vlen, byte[] key) {
        this.fileId = fileId;
        this.offset = offset;
        this.klen = klen;
        this.vlen = vlen;
        this.key = key;
    }
}
