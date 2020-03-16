package bitcask;

import config.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import store.WrappedBytes;

import java.io.File;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

class Compactor {
    private static Logger LOG = LogManager.getLogger(Compactor.class);

    private static final AtomicInteger compactId = new AtomicInteger(0);

    /**
     * compact输入文件
     * @param dataDir  目标文件夹
     * @param operateFiles  待compact文件   会关闭该文件
     * @return 返回一个临时的DataFile  已关闭
     * @throws Exception
     */
    public static DataFile compact(File dataDir, List<DataFile> operateFiles) throws Exception {
        DataFile lastDataFile = operateFiles.get(operateFiles.size() - 1);
        DataFile temp = new DataFile(new File(dataDir, DataFile.newDataFileNameById(lastDataFile.id + Config.FILE_STEP - 1)), Integer.MAX_VALUE, false);
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
        temp.writeMap(treeMap);
        temp.setIndexMap(SparseIndexBuilder.buildIndexFileIfNotExists(temp, true));
        return temp;
    }

}
