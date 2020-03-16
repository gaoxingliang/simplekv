package config;

public class Config {
    /**
     * Server 启动端口
     */
    public static final int SERVER_PORT = 6666;


    /**
     * cmd code
     */
    public static final int MAGIC_NUMER_METRIC = 12345678;
    public static final int MAGIC_NUMER_READ_KEY = 22345678;
    public static final int MAGIC_NUMER_PUT_KEY = 32345678;

    public static final int MAGIC_NUMBER_LASTWRITE = 0x1111EEEE;

    /**
     * 至少这么多个文件才开始compact?
     */
    public static final int COMPACT_FILE_COUNT_THRESHOLD = 2;

    /**
     * 每个data文件有多少个索引记录
     */
    public static final int INDEX_RECORD_PER_FILE =  1000;

    /**
     * 当内存map达到多大(估计)后dump到文件
     */
    public static final int DUMP_MEM_THRESHOLD_OF_MAP_MEM = 1024 * 1024 * 20;

    /**
     * datafile的步进id  那么可能我们可以同时有多个compactor线程来做?
     */
    public static final int FILE_STEP = 2; // 决定了compactor以后可以并行的来做?

}
