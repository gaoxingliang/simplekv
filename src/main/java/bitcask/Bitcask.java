package bitcask;

import config.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import store.KVStore;
import store.Metrics;
import store.WrappedBytes;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class Bitcask implements KVStore {
    private static Logger LOG = LogManager.getLogger(Bitcask.class);

    private ReentrantReadWriteLock mu = new ReentrantReadWriteLock(true);
    private Lock wlock = mu.writeLock();
    private Lock rlock = mu.writeLock();
    private MemTable currentMemTable;
    private MemTable transactionalMemTable;
    private final AtomicBoolean memTable2DataSubmited = new AtomicBoolean(false);

    private DataFile walFile;
    private File path;

    private TreeMap<Integer, DataFile> dataFilesMap = new TreeMap<>();

    private Metrics metrics = new Metrics();

    private ScheduledExecutorService coExecutor;
    @Override
    public void open() throws Exception {
        long startLoad = System.currentTimeMillis();
        String pathString = "bitcaskDataDir";
        File basepath = new File(pathString);
        if (!basepath.exists() && !new File(pathString).mkdirs()) {
            throw new IllegalStateException("Fail to create " + pathString);
        }
        this.path = basepath;

        /***
         * load all files
         */
        wlock.lock();
        try {
            dataFilesMap = _loadDataFiles();
            currentMemTable = new MemTable();
            // load wal logs'
            createWALFile(false);
            loadWAL2Mem();
        }
        finally {
            wlock.unlock();
        }
        LOG.info("Finished loading cost time={}ms, indexMapCount={}, fileCount={}", System.currentTimeMillis() - startLoad,
                currentMemTable.size(), dataFilesMap.size());
        startCO();
    }

    private void loadWAL2Mem() throws Exception  {
        Entry en = null;
        while ((en = walFile.readNext()) != null) {
            currentMemTable.put(en.key, en.value);
        }
    }

    private void createWALFile(boolean recreate) throws Exception {
        if (recreate && walFile != null) {
            walFile.close();
            walFile.file.delete();
        }
        File wal = new File(path, "wal.log");
        if (!wal.exists()) {
            wal.createNewFile();
        }
        walFile = new DataFile(wal, -1, false);
    }


    private void startCO() {
        coExecutor = Executors.newSingleThreadScheduledExecutor();
        coExecutor.scheduleAtFixedRate(() -> checkAndCompactIfPossible(), 10, 60, TimeUnit.SECONDS);
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
            DataFile tempcomapctFile = Compactor.compact(ops.get(0).file.getParentFile(), ops);
            LOG.info("Finished compact, cost={}ms, file={}", tempcomapctFile.file.getName());
            synchronized (dataFilesMap) {
                ops.stream().map(o -> dataFilesMap.get(o.id)).forEach(oldFile -> {
                    try {
                        oldFile.close();
                        oldFile.file.delete();
                        Optional.ofNullable(oldFile.indexFile).ifPresent(o -> o.delete());
                        dataFilesMap.remove(oldFile.id);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                dataFilesMap.put(tempcomapctFile.id, tempcomapctFile);
            }
            LOG.info("Index map is rebuild, costTime={}ms for {} files", System.currentTimeMillis() - startCompact, ops.size());
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
                return new DataFile(f, DataFile.parseIdFromDataFile(f.getName()), true);
            }
            catch (FileNotFoundException e) {
                throw new IllegalStateException(e);
            }
        }).sorted(Comparator.comparingInt(d -> d.id)).forEach(d -> {
            map.put(d.id, d);
            try {
                d.indexMap = SparseIndexBuilder.buildIndexFileIfNotExists(d, true);
            }
            catch (Exception e) {
                LOG.error("Fail to build index file - {}", d.file, e);
            }
        });
        return map;
    }

    public byte[] get(byte[] k) throws Exception {
        long startNano = System.nanoTime();
        boolean got = false;
        rlock.lock();
        try {
            byte [] v = currentMemTable.get(k);
            // 可能有一个new dump的task

            if (v == null) {
                if (transactionalMemTable != null) {
                    v = transactionalMemTable.get(k);
                }
                if (v == null) {
                    // 从最后往最前
                    List<DataFile> list = new ArrayList<>();
                    synchronized (dataFilesMap) {
                        list.addAll(dataFilesMap.values());
                    }
                    for (int j = list.size() - 1; j > 0; j--) {
                        DataFile cur = list.get(j);
                        v = cur.search(k);
                        if (v != null) {
                            break;
                        }
                    }
                }
            }
            got = v != null;
            return v;
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


    @Override
    public void close() throws Exception {
        wlock.lock();
        try {
            for (DataFile d : dataFilesMap.values()) {
                d.close();
            }
            coExecutor.shutdown();
            coExecutor.awaitTermination(1, TimeUnit.MINUTES);
        }
        finally {
            wlock.unlock();
        }
    }

    public void put(byte[] k, byte[] v) throws Exception {
        //LOG.debug("Start puting k={}", new String(k));
        long startNano = System.nanoTime();
        wlock.lock();
        try {
            walFile.write(new Entry(WrappedBytes.of(k), v));
            currentMemTable.put(k, v);
            if (currentMemTable.needDump() && memTable2DataSubmited.compareAndSet(false, true)) {
                MemTable newMem = new MemTable();
                transactionalMemTable = currentMemTable;
                currentMemTable = newMem;
                submitNewDumpTask(transactionalMemTable);
            }
        }
        finally {
            wlock.unlock();
        }
        metrics.flagPut(System.nanoTime() - startNano);
    }


    private void submitNewDumpTask(MemTable memTable) throws Exception {
        coExecutor.submit(new Runnable() {
            @Override
            public void run() {
                LOG.info("Current memtable need to dump");
                // let's dump now
                int newId = 0;
                try {
                    synchronized (dataFilesMap) {
                        newId = dataFilesMap.isEmpty() ? 0 : dataFilesMap.lastKey() + Config.FILE_STEP;
                    }
                    File newFile = new File(path, DataFile.newDataFileNameById(newId));
                    newFile.createNewFile();
                    DataFile newDataFile = DataFile.newDataFile(newFile, newId, false);
                    memTable.dump2File(newDataFile);
                    newDataFile.close();
                    newDataFile = DataFile.newDataFile(newFile, newId, true);
                    newDataFile.indexMap = SparseIndexBuilder.buildIndexFileIfNotExists(newDataFile, true);

                    synchronized (dataFilesMap) {
                        dataFilesMap.put(newId, newDataFile);
                    }
                    // 清理WAL
                    createWALFile(true);
                    memTable.clear();
                } catch (Exception e) {
                    LOG.error("Fail to dump to new data file", e);
                } finally {
                    memTable2DataSubmited.set(false);
                }
            }
        });

    }

}
