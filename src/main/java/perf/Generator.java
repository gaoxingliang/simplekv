package perf;

import store.KVStore;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

/**
 * 生成测试文件
 */
public class Generator {
    private static final int charscount = 26;
    private static final char [] chars = new char[charscount];
    static {
        for (int i = 0; i < charscount; i ++) {
            chars[i] = (char)('A' + i);
        }
    }

    public static byte [] newRandomChars(int len, int startChar, int endChar) {
        Random r = new Random();
        byte [] bs = new byte[len];
        for (int i = 0; i < len; i++) {
            bs[i] = (byte)chars[startChar + r.nextInt(endChar - startChar)];
        }
        return bs;
    }

    public static File generatePerfTestFile(int recordCount) throws IOException {
        File t = new File("perftestFile" + recordCount);
        if (t.exists()) {
            return t;
            //t.delete();
        }
        t.createNewFile();
        Random r = new Random();
        int startEnd [] = {1, 10, 5, 15, 10, 25};
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(t));
        for (int i = 0; i < recordCount; i++) {
            int klen = Math.max(5, r.nextInt(KVStore.MAX_KEY_LEN));
            int vlen = Math.max(5, r.nextInt(KVStore.MAX_VALUE_LEN));
            ByteBuffer bs = ByteBuffer.allocate(8 + klen + vlen);
            bs.putInt(klen);
            bs.put(newRandomChars(klen, startEnd[(i%3) * 2], startEnd[(i%3) * 2 + 1]));
            bs.putInt(vlen);
            for (int j = 0; j < vlen; j++) {
                bs.put((byte)chars[r.nextInt(charscount)]);
            }
            bs.flip();
            dos.write(bs.array());
        }
        dos.flush();
        dos.close();
        return t;
    }
    public static void main(String[] args) throws IOException {
        generatePerfTestFile(100000);
        generatePerfTestFile(1000000);
        generatePerfTestFile(5000000);
    }



}
