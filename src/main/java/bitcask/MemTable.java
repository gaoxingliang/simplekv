package bitcask;

import config.Config;
import store.WrappedBytes;

import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class MemTable {
    private TreeMap<WrappedBytes, byte[]> treeMap = new TreeMap<>();
    private long lastCalculate = 0;
    private AtomicBoolean isCalculating = new AtomicBoolean(false);
    private int lastCalculateSize = 0;

    public MemTable() {
    }

    public synchronized void put(byte[] k, byte[] v) {
        treeMap.put(WrappedBytes.of(k), v);
    }

    public synchronized void put(WrappedBytes k, byte[] v) {
        treeMap.put(k, v);
    }

    public synchronized int size() {
        return treeMap.size();
    }

    public synchronized byte[] get(byte[] k) {
        return treeMap.get(WrappedBytes.of(k));
    }

    public synchronized boolean needDump() {
        if (System.currentTimeMillis() - lastCalculate < 10 * 1000) {
            return lastCalculateSize >= Config.DUMP_MEM_THRESHOLD_OF_MAP_MEM;
        }
        int bytes = treeMap.entrySet().stream().mapToInt(e -> (e.getKey().bytes.length + e.getValue().length))
                .sum();
        bytes = (int) (bytes * 1.2);
        lastCalculateSize = bytes;
        lastCalculate = System.currentTimeMillis();
        return bytes >= Config.DUMP_MEM_THRESHOLD_OF_MAP_MEM;
    }

    public synchronized void dump2File(DataFile df) throws Exception {
        treeMap.entrySet().forEach(e -> {
            try {
                df.write(new Entry(e.getKey(), e.getValue()));
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }
        });
    }

    public synchronized void clear() {
        treeMap.clear();
    }

}
