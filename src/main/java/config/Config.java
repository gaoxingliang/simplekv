package config;

public class Config {
    public static final int SERVER_PORT = 6666;
    public static final int MAGIC_NUMER_METRIC = 12345678;
    public static final int MAGIC_NUMER_READ_KEY = 22345678;
    public static final int MAGIC_NUMER_PUT_KEY = 32345678;

    public static final int MAGIC_NUMBER_LASTWRITE = 0x1111EEEE;

    public static final long FREEZE_THRESHOLD = 1024 * 1024 * 10; // 30MB
    public static final int INDEX_PER_FILE = 1000;

    public static final int COMPACT_FILE_COUNT_THRESHOLD = 2;
}
