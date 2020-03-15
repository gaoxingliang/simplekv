package bbbb;

import config.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import store.KVStore;
import store.Metrics;
import store.WrappedBytes;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class Bitcask implements KVStore {
    private ReentrantReadWriteLock mu = new ReentrantReadWriteLock(true);
    private Lock wlock = mu.writeLock();
    private Lock rlock = mu.writeLock();
    private DataFile curr;
    TreeMap<WrappedBytes, Item> trieMap = new TreeMap();
    Indexer indexer = new Indexer();
    private File path;
    private File tempDir;
    private File indexFile;

    private TreeMap<Integer, DataFile> dataFilesMap = new TreeMap<>();

    private Metrics metrics = new Metrics();
    private static Logger LOG = LogManager.getLogger(Bitcask.class);

    @Override
    public void open() throws Exception {
        long startLoad = System.currentTimeMillis();
        String pathString = "bitcaskDataDir";
        File basepath = new File(pathString);
        if (!basepath.exists() && !new File(pathString).mkdirs()) {
            throw new IllegalStateException("Fail to create " + pathString);
        }
        this.path = basepath;
        tempDir = new File(path, "temp");
        if (!tempDir.exists()) {
            tempDir.mkdirs();
        }
        makeSureIndexFile();
        /***
         * load all files
         */
        wlock.lock();
        try {
            dataFilesMap = _loadDataFiles();
            trieMap = loadIndex(dataFilesMap);
            int lastId = dataFilesMap.isEmpty() ? 0 : dataFilesMap.lastKey();
            curr = new DataFile(new File(path, newDataFileNameById(lastId + 1)), lastId + 1, false);
        }
        finally {
            wlock.unlock();
        }
        LOG.info("Finished loading cost time={}ms, indexMapCount={}, fileCount={}", System.currentTimeMillis() - startLoad,
                trieMap.size(), dataFilesMap.size());
        startCompactor();
    }


    private void startCompactor() {
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(()-> checkAndCompactIfPossible(), 10, 60, TimeUnit.SECONDS);
    }
    private void checkAndCompactIfPossible() {
        List<DataFile> ops;
        synchronized (dataFilesMap) {
            if (dataFilesMap.size() < Config.COMPACT_FILE_COUNT_THRESHOLD) {
                LOG.info("Not compact no");
                return;
            }
            // choose the lower keys
            ops = dataFilesMap.values().stream().limit(dataFilesMap.size() - Config.COMPACT_FILE_COUNT_THRESHOLD)
                    .map(r -> {
                        try {
                            return DataFile.copy(r);
                        }
                        catch (FileNotFoundException e) {
                            throw new IllegalStateException(e);
                        }
                    }).collect(Collectors.toList());
        }
        try {
            long startCompact = System.currentTimeMillis();
            Pair<DataFile, TreeMap<WrappedBytes, Entry>> pair = Compactor.compact(tempDir, ops);
            DataFile tempcomapctFile = pair.l;
            TreeMap<WrappedBytes, Entry> tempMap = pair.r;
            LOG.info("Finished compact, cost={}ms, file={}", tempcomapctFile.file.getName());
            synchronized (dataFilesMap) {
                ops.forEach(op -> {
                    DataFile oldFile = dataFilesMap.remove(op.id);
                    try {
                        oldFile.close();
                        oldFile.file.delete();
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                int newId = ops.get(ops.size() - 1).id; // 最后一个的id
                File newFile = new File(path, newDataFileNameById(newId));
                tempcomapctFile.file.renameTo(newFile);
                dataFilesMap.put(newId, new DataFile(newFile, newId, true));
                /**
                 * rebuild index map
                 */
                synchronized (trieMap) {
                    tempMap.values().forEach(e -> {
                        trieMap.put(e.key, new Item(newId, e.offset, e.key.bytes.length, e.value.length, e.key.bytes));
                    });
                }

                // !todo index file?
            }
            LOG.info("Index map is rebuild");
        }
        catch (Exception e) {
            LOG.error("Fail to compact", e);
        }

    }

    private TreeMap<Integer, DataFile> _loadDataFiles() {
        List<File> fileList = Arrays.asList(this.path.listFiles(f -> f.getName().endsWith("data")));
        TreeMap<Integer, DataFile> map = new TreeMap<>();
        fileList.stream().map(f -> {
            try {
                return new DataFile(f, parseIdFromDataFile(f.getName()), true);
            }
            catch (FileNotFoundException e) {
                throw new IllegalStateException(e);
            }
        }).sorted(Comparator.comparingInt(d -> d.id)).forEach(d -> map.put(d.id, d));
        return map;
    }

    private DataFile newDataFile(File path, int id, boolean readonly) throws FileNotFoundException {
        return new DataFile(path, id, readonly);
    }

    private int parseIdFromDataFile(String fileName) {
        return Integer.valueOf(fileName.substring(0, fileName.length() - ".data".length()));
    }

    private String newDataFileNameById(int id) {
        return id + ".data";
    }

    private TreeMap loadIndex(TreeMap<Integer, DataFile> dataFiles) throws Exception {
        File indexFile = new File(path, "index");
        if (!indexFile.exists()) {
            // 读取每个文件的第一个
            TreeMap<WrappedBytes, Item> t = new TreeMap<>();
            for (DataFile d : dataFiles.values()) {
                Entry e = null;
                while ((e = d.readNext()) != null) {
                    // int fileId, long offset, int klen, int vlen, byte[] key
                    t.put(e.key, new Item(d.id, e.offset, e.key.bytes.length, e.value.length, e.key.bytes));
                }
            }
            return t;
        }
        else {
            return indexer.load(indexFile.getAbsolutePath());
        }
    }

    public byte[] get(byte[] k) throws Exception {
        long startNano = System.nanoTime();
        boolean got = false;
        rlock.lock();
        try {
            Item item;
            synchronized (trieMap) {
                 item = trieMap.get(new WrappedBytes(k));
            }
            if (item == null) {
                return null;
            }
            Entry en;
            if (item.fileId == curr.id) {
                en = curr.readAt(item.offset);
            }
            else {
                DataFile f;
                synchronized (dataFilesMap) {
                    f = dataFilesMap.get(item.fileId);
                }
                en = f.readAt(item.offset);
            }
            got = en != null;
            return en == null ? null : en.value;
        }
        finally {
            rlock.unlock();
            metrics.flagGet(got, System.nanoTime() - startNano);
        }
    }

    @Override
    public Metrics metrics() {
        return metrics;
    }

    private void makeSureIndexFile() throws IOException {
        indexFile = new File(path, "index");
        if (!indexFile.exists()) {
            indexFile.createNewFile();
        }
    }


    @Override
    public void close() throws Exception {
        wlock.lock();
        try {
            curr.close();
            for (DataFile d : dataFilesMap.values()) {
                d.close();
            }
            indexer.save(trieMap, indexFile.getAbsolutePath());
        }
        finally {
            wlock.unlock();
        }
    }

    public void put(byte[] k, byte[] v) throws Exception {
        long startNano = System.nanoTime();
        wlock.lock();
        try {
            long s = curr.size();
            if (s > Config.FREEZE_THRESHOLD) {
                curr.close();
                int id = curr.id;
                DataFile freeze = newDataFile(curr.file, id, true);
                synchronized (dataFilesMap) {
                    dataFilesMap.put(id, freeze);
                }
                curr = newDataFile(new File(path, newDataFileNameById(id + 1)), id + 1, false);
            }
            WrappedBytes kb = new WrappedBytes(k);
            long offset = curr.write(new Entry(kb, v));
            // int fileId, long offset, int klen, int vlen, byte[] key
            synchronized (trieMap) {
                trieMap.put(kb, new Item(curr.id, offset, kb.bytes.length, v.length, kb.bytes));
            }
        }
        finally {
            wlock.unlock();
        }
        metrics.flagPut(System.nanoTime() - startNano);
    }

}
