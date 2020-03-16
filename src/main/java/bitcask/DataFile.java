package bitcask;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import store.WrappedBytes;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DataFile {
    private static Logger LOG = LogManager.getLogger(DataFile.class);

    int id;
    Lock wlock;
    Lock rlock;
    private FileChannel writeChannel;  // 追加写
    private FileChannel readChannel;  // 随机读
    private FileChannel seqReadChannel; // 顺序读
    File file;
    File indexFile;
    volatile long filelength;
    final boolean readonly;
    volatile boolean closed = false;

    volatile TreeMap<WrappedBytes, IndexItem> indexMap;


    public DataFile(File path, int id, boolean readonly) throws FileNotFoundException {
        this.readonly = readonly;
        if (!readonly) {
            // read and write
            wlock = new ReentrantLock();
            writeChannel = new FileOutputStream(path, true).getChannel();
        }
        readChannel = new RandomAccessFile(path, "r").getChannel();
        seqReadChannel = new FileInputStream(path).getChannel();
        rlock = new ReentrantLock();
        filelength = path.length();
        this.id = id;
        this.file = path;
    }

    public void close() throws Exception {
        _notClosed();
        if (wlock != null) {
            wlock.lock();
            IOUtils.closeQuietly(writeChannel);
            wlock.unlock();
        }

        rlock.lock();
        IOUtils.closeQuietly(seqReadChannel);
        rlock.unlock();

        IOUtils.closeQuietly(readChannel);
        closed = true;
        LOG.info("File id={} file={} is closed", id, file.getName());

    }

    private void _notClosed() {
        if (closed) {
            throw new IllegalStateException("The file has been closed - " + file.getAbsolutePath());
        }
    }

    public void setIndexMap(TreeMap<WrappedBytes, IndexItem> indexMap) {
        this.indexMap = indexMap;
    }

    public byte[] search(byte[] k) throws Exception {
        _notClosed();
        WrappedBytes w = WrappedBytes.of(k);
        Map.Entry<WrappedBytes, IndexItem> minEntry = null;
        synchronized (indexMap) {
            // >=
            // 找到一个最大的 和 最小的
            // <=
            minEntry = indexMap.floorEntry(w);
            if (minEntry == null) {
                // 所有的都比当前的大
                return null;
            }
        }
        long startSearch = minEntry.getValue().offset;

        while (true) {
            // !todo use a seq scan?
            Entry en = this.readAt(startSearch);
            if (en == null) {
                return null;
            }
            else {
                int res = WrappedBytes.comparing(k, en.key.bytes);
                if (res == 0) {
                    return en.value;
                }
                else if (res < 0) {
                    return null;
                }
            }
        }
    }

    /**
     * 写入一个Entry 返回写入开始的offset
     *
     * @param entry
     * @return
     * @throws Exception
     */
    public long write(Entry entry) throws Exception {
        _notClosed();
        wlock.lock();
        long beforeOffset = writeChannel.position();
        encode2Bytes(entry, writeChannel);
        filelength = writeChannel.position();
        wlock.unlock();
        return beforeOffset;
    }

    public void writeMap(TreeMap<WrappedBytes, Entry> treeMap) throws Exception {
        _notClosed();
        wlock.lock();
        treeMap.entrySet().forEach(entry -> {
            try {
                encode2Bytes(entry.getValue(), writeChannel);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        });

        filelength = writeChannel.position();
        wlock.unlock();
    }

    private void encode2Bytes(Entry entryNoOffset, FileChannel ch) throws IOException {
        /**
         * [keyLenth, valueLen, key, value]
         */
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putInt(entryNoOffset.key.bytes.length);
        buffer.putInt(entryNoOffset.value.length);
        buffer.flip();
        ByteBuffer kvBuf = ByteBuffer.allocate(entryNoOffset.key.bytes.length + entryNoOffset.value.length);
        kvBuf.put(entryNoOffset.key.bytes);
        kvBuf.put(entryNoOffset.value);
        kvBuf.flip();
        ch.write(buffer);
        ch.write(kvBuf);
    }

    public Entry readNext() throws Exception {
        _notClosed();
        rlock.lock();
        // [keyLenth, valueLen, key, value]
        ByteBuffer buffer = ByteBuffer.allocate(8);
        long beforePos = seqReadChannel.position();
        seqReadChannel.read(buffer);
        if (buffer.hasRemaining()) {
            seqReadChannel.position(beforePos);
            return null;
        }
        buffer.flip();
        int klen = buffer.getInt(), vlen = buffer.getInt();
        ByteBuffer kvBuf = ByteBuffer.allocate(klen + vlen);
        seqReadChannel.read(kvBuf);
        if (kvBuf.hasRemaining()) {
            seqReadChannel.position(beforePos);
            return null;
        }
        rlock.unlock();
        kvBuf.flip();
        byte[] ks = new byte[klen];
        byte vs[] = new byte[vlen];
        kvBuf.get(ks);
        kvBuf.get(vs);
        return new Entry(new WrappedBytes(ks), vs, beforePos);
    }

    public Entry readAt(long offset) throws Exception {
        _notClosed();
        /**
         * [keyLenth, valueLen, key, value]
         */
        ByteBuffer buffer = ByteBuffer.allocate(8);
        readChannel.read(buffer, offset);
        if (buffer.hasRemaining()) {
            return null;
        }
        buffer.flip();
        int klen = buffer.getInt(), vlen = buffer.getInt();
        ByteBuffer kvBuf = ByteBuffer.allocate(klen + vlen);
        readChannel.read(kvBuf, offset + 8);
        if (kvBuf.hasRemaining()) {
            return null;
        }
        kvBuf.flip();
        byte[] ks = new byte[klen];
        byte vs[] = new byte[vlen];
        kvBuf.get(ks);
        kvBuf.get(vs);
        return new Entry(new WrappedBytes(ks), vs, offset);
    }

    public long size() {
        return filelength;
    }

    public static DataFile copy(DataFile f) throws FileNotFoundException {
        DataFile newDataFile = new DataFile(f.file, f.id, true);
        return newDataFile;
    }


    public static DataFile newDataFile(File path, int id, boolean readonly) throws FileNotFoundException {
        return new DataFile(path, id, readonly);
    }

    public static int parseIdFromDataFile(String fileName) {
        return Integer.valueOf(fileName.substring(0, fileName.length() - ".data".length()));
    }

    public static String newDataFileNameById(int id) {
        return id + ".data";
    }
}
