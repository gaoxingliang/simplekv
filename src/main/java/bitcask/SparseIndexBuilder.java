package bitcask;

import config.Config;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import store.WrappedBytes;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

/**
 * 根据某个datafile构建稀疏索引
 */
public class SparseIndexBuilder {
    // 负责索引的文件
    // 与DataFile绑定
    // 格式如下:

    // totalRecordCount, sampleRecordCount
    // record1 [klen, vlen, offset, key]
    // record2 [klen, vlen, offset, key]
    // ...
    // ]
    private static Logger LOG = LogManager.getLogger(SparseIndexBuilder.class);


    public static TreeMap<WrappedBytes, IndexItem> buildIndexFileIfNotExists(DataFile df, boolean needIndexFile) throws Exception {
        // 根据某个文件生成index文件
        if (!df.readonly) {
            throw new IllegalStateException("Not read only file " + df);
        }

        File indexFile = indexFileOfDataFile(df);
        TreeMap<WrappedBytes, IndexItem> map = new TreeMap<>();
        if (needIndexFile && indexFile.exists()) {
            LOG.info("Index file exists for {}, let's build the sparse index", df.file);
            //!todo 检查完整性

            FileChannel fc = null;
            try {
                fc = new FileInputStream(indexFile).getChannel();
                MappedByteBuffer b = fc.map(FileChannel.MapMode.READ_ONLY, 0, indexFile.length());
                int totalRecordCount = b.getInt(), sampleRecordCount = b.getInt();
                LOG.info("Will load total={}, sample={} for file={}", totalRecordCount, sampleRecordCount, indexFile.getName());
                for (int i = 0; i < sampleRecordCount; i++) {
                    IndexItem e = getFromBuffer(b, df);
                    map.put(WrappedBytes.of(e.key), e);
                }
            }
            finally {
                IOUtils.closeQuietly(fc);
            }
        }
        else {
            // 生成index file
            if (needIndexFile) {
                indexFile.createNewFile();
                LOG.info("The index file not exists, let's build and create for {}", df.file);
            }
            else {
                LOG.info("Build a memory index, let's build and create for {}", df.file);
            }
            DataFile copy = DataFile.copy(df);
            Entry en = null;
            List<IndexItem> indexList = new ArrayList<>(Config.INDEX_RECORD_PER_FILE);
            /**
             * 蓄水池算法来抽样
             */
            Random r = new Random();
            int i = 0, m = 0;
            while ((en = copy.readNext()) != null) {
                i++;
                if (indexList.size() < Config.INDEX_RECORD_PER_FILE) {
                    // int fileId, long offset, int klen, int vlen, byte[] key
                    indexList.add(new IndexItem(df.id, en.offset, en.key.bytes.length, en.value.length, en.key.bytes));
                }
                else {
                    m = r.nextInt(i);
                    if (m < Config.INDEX_RECORD_PER_FILE) {
                        indexList.set(m, new IndexItem(df.id, en.offset, en.key.bytes.length, en.value.length, en.key.bytes));
                    }
                }
            }
            copy.close();
            if (!needIndexFile) {
                indexList.forEach(index -> map.put(WrappedBytes.of(index.key), index));
                LOG.info("Mem index is build for {}", df.file);
            }
            else {
                // build 完成写文件
                FileChannel writeChannel = null;
                try {
                    writeChannel = new FileOutputStream(indexFile).getChannel();
                    ByteBuffer stats = ByteBuffer.allocate(8);
                    stats.putInt(i).putInt(indexList.size());
                    stats.flip();
                    writeChannel.write(stats);
                    for (IndexItem index : indexList) {
                        writeChannel.write(indexItem2Buffer(index));
                        map.put(WrappedBytes.of(index.key), index);
                    }
                }
                finally {
                    IOUtils.closeQuietly(writeChannel);
                }
                df.indexFile = indexFile;
                LOG.info("Index file for {} is build", df.file);
            }
        }
        return map;
    }

    private static ByteBuffer indexItem2Buffer(IndexItem i) {
        // [klen, vlen, offset, key]
        ByteBuffer byteBuffer = ByteBuffer.allocate(8 + 8 + i.klen);
        byteBuffer.putInt(i.klen)
                .putInt(i.vlen)
                .putLong(i.offset)
                .put(i.key);
        LOG.debug("item2buf,klen={},vlen={},offset={}", i.klen, i.vlen, i.offset);
        byteBuffer.flip();
        return byteBuffer;

    }

    private static IndexItem getFromBuffer(ByteBuffer buf, DataFile df) {
        // [klen, vlen, offset, key]
        int klen = buf.getInt(), vlen = buf.getInt();
        long offset = buf.getLong();
        LOG.debug("getFromBuffer mfile={},klen={},vlen={},offset={}", df.file, klen, vlen, offset);
        byte[] ks = new byte[klen];
        buf.get(ks);
        // int fileId, long offset, int klen, int vlen, byte[] key
        IndexItem item = new IndexItem(df.id, offset, klen, vlen, ks);
        return item;
    }


    private static File indexFileOfDataFile(DataFile df) {
        return new File(df.file.getParent(), df.file.getName() + ".index");
    }

}
