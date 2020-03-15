package bbbb;

import java.nio.ByteBuffer;

public class Util {


    public static long bytes2Long(byte [] bytes) {
        return ByteBuffer.wrap(bytes).getLong();
    }

    public static byte[] long2bytes(long v) {
        return ByteBuffer.allocate(8).putLong(v).array();
    }

    public static int bytes2Int(byte[] bytes) {
        //如果不与0xff进行按位与操作，转换结果将出错，有兴趣的同学可以试一下。
        int int1 = bytes[0] & 0xff;
        int int2 = (bytes[1] & 0xff) << 8;
        int int3 = (bytes[2] & 0xff) << 16;
        int int4 = (bytes[3] & 0xff) << 24;

        return int1 | int2 | int3 | int4;
    }

    public static byte[] int2Bytes(int integer) {
        byte[] bytes = new byte[4];
        bytes[3] = (byte) (integer >> 24);
        bytes[2] = (byte) (integer >> 16);
        bytes[1] = (byte) (integer >> 8);
        bytes[0] = (byte) (integer);

        return bytes;
    }
}
