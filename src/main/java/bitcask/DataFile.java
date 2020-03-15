package bitcask;

import org.apache.commons.io.IOUtils;
import store.WrappedBytes;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DataFile {

    int id;
    Lock wlock;
    Lock rlock;
    FileChannel writeChannel;  // 追加写
    FileChannel readChannel;  // 随机读
    FileChannel seqReadChannel; // 顺序读
    File file;
    volatile long filelength;

    public DataFile(File path, int id, boolean readonly) throws FileNotFoundException {
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
        if (wlock != null) {
            wlock.lock();
            IOUtils.closeQuietly(writeChannel);
            wlock.unlock();
        }

        rlock.lock();
        IOUtils.closeQuietly(seqReadChannel);
        rlock.unlock();

        IOUtils.closeQuietly(readChannel);
    }

    public void sync() throws Exception {

    }

    /**
     * 写入一个Entry 返回写入开始的offset
     * @param entry
     * @return
     * @throws Exception
     */
    public long write(Entry entry) throws Exception {
        wlock.lock();
        long beforeOffset = writeChannel.position();
        encode2Bytes(entry, writeChannel);
        filelength = writeChannel.position();
        wlock.unlock();
        return beforeOffset;
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
        byte [] ks = new byte[klen];
        byte vs[] = new byte[vlen];
        kvBuf.get(ks);
        kvBuf.get(vs);
        return new Entry(new WrappedBytes(ks), vs, beforePos);
    }

    public Entry readAt(long offset) throws Exception {
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
        byte [] ks = new byte[klen];
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
}
