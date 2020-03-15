package bitcask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import store.WrappedBytes;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

class Compactor {
    private static Logger LOG = LogManager.getLogger(Compactor.class);

    private static final AtomicInteger compactId = new AtomicInteger(0);
    public static Pair<DataFile, TreeMap<WrappedBytes, Entry>> compact(File tempFolder, List<DataFile> operateFiles) throws Exception {
        DataFile temp = new DataFile(new File(tempFolder, compactId.incrementAndGet() + ""), Integer.MAX_VALUE, false);
        TreeMap<WrappedBytes, Entry> treeMap = new TreeMap<>();
        Entry en = null;
        for (DataFile d : operateFiles) {
            LOG.info("Start compacting file : {}", d.file.getName());
            while ((en = d.readNext()) != null) {
              //  LOG.info("Current pos {}", en.offset);
                treeMap.put(en.key, en);
            }
            d.close();
        }
        Iterator<Entry> it = treeMap.values().iterator();
        long offset = 0;
        while (it.hasNext()) {
            en = it.next();
            en.offset = offset;
            temp.write(en);
            offset += en.writeSize();
        }
        temp.close();
        return new Pair<>(temp, treeMap);
    }
}
