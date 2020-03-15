package bitcask;

import org.apache.commons.io.IOUtils;
import store.WrappedBytes;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.TreeMap;

/**
 * 索性文件结构:
 *
 *  // [klen, vlen, fileId, offset, key]
 *
 */
public class Indexer {
    private static final int intSize = 4, longSize = 8, fileIdSize = 4, offsetSize = 8, sizeSize = 4;
    // klen, vlen,  fileId, offset
    private static final int MINIMAL_READ = 4 + 4 + 4 + 8;

    public TreeMap<WrappedBytes, Item> load(String path) throws Exception {
        File indexFile = new File(path);
        if (!indexFile.exists()) {
            throw new IllegalStateException("the file not exists " + path);
        }
        TreeMap<WrappedBytes, Item> t = new TreeMap<WrappedBytes, Item>();
        FileChannel in = null;
        try {
            in = new FileInputStream(indexFile).getChannel();
            //[klen, vlen, fileId, offset, key]
            ByteBuffer metaBuf = ByteBuffer.allocate(MINIMAL_READ);
            while (true) {
                metaBuf.clear();
                in.read(metaBuf);
                if (metaBuf.hasRemaining()) {
                    break;
                }
                metaBuf.flip();
                int klen = metaBuf.getInt(), vlen = metaBuf.getInt(), fileId = metaBuf.getInt();
                long offset = metaBuf.getLong();
                ByteBuffer kBuf = ByteBuffer.allocate(klen);
                in.read(kBuf);
                if (kBuf.hasRemaining()) {
                    break;
                }
                kBuf.flip();
                byte [] ks = new byte[klen];
                kBuf.get(ks);
                // int fileId, long offset, int klen, int vlen, byte[] key
                Item item = new Item(fileId, offset, klen, vlen, ks);
                t.put(new WrappedBytes(ks), item);
            }
            return t;
        } finally {
            IOUtils.closeQuietly(in);
        }
    }



    public void save(TreeMap<WrappedBytes, Item> t, String path) throws Exception {
        File indexFile = new File(path);
        if (!indexFile.exists()) {
            indexFile.createNewFile();
        }
        FileChannel fc = null;
        try {
            fc = new FileOutputStream(indexFile).getChannel();
            FileChannel finalDos = fc;
            t.entrySet().forEach(e -> {
                try {
                    // [klen, vlen,  fileId, offset, key]
                    byte[] k = e.getKey().bytes;
                    ByteBuffer metaBuf = ByteBuffer.allocate(MINIMAL_READ + k.length);
                    Item v = e.getValue();
                    metaBuf.putInt(k.length)
                            .putInt(v.vlen)
                            .putInt(v.fileId)
                            .putLong(v.offset)
                            .put(k);
                    metaBuf.flip();
                    finalDos.write(metaBuf);
                }
                catch (IOException ex) {
                    throw new IllegalStateException("Fail to write ", ex);
                }
            });
        } finally {
            IOUtils.closeQuietly(fc);
        }


    }

}
