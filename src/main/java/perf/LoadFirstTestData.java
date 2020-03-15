package perf;

import bitcask.Bitcask;
import store.KVStore;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LoadFirstTestData {

    public static void main(String[] args) throws Exception{
//        long [] r1 = test(new SimpleKVStore());
//        System.out.println(Arrays.toString(r1));

        long [] r2 = test(new Bitcask());
        System.out.println(Arrays.toString(r2));
    }

    // load time, query time
    public static long[] test(KVStore s) throws Exception {
        //s.open();
        long[] res = new long[2];
        long startLoad = System.currentTimeMillis();
        s.open();
        File f = new File("perftestFile1000000");
        FileChannel fileChannel = new FileInputStream(f).getChannel();
        ByteBuffer buf = ByteBuffer.allocate(1024);
        System.out.println("Size: " + fileChannel.size() / 1024/ 1024 + "MB");
        int i= 0;
        MappedByteBuffer b = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, f.length());
        int klen, vlen;
        byte [] ks = new byte[1024];
        byte vs[] = new byte[1024];

        List<byte[]> kvs = new ArrayList<>();
        int k = 0;
        while (b.hasRemaining()) {
            klen = b.getInt();
            b.get(ks, 0, klen);
            vlen = b.getInt();
            b.get(vs, 0, vlen);
            s.put(Arrays.copyOfRange(ks, 0, klen), Arrays.copyOfRange(vs, 0, vlen));
            if (++k  % 10000 == 0) {
                kvs.add(Arrays.copyOfRange(ks, 0, klen));
                kvs.add(Arrays.copyOfRange(vs, 0, vlen));
            }
        }

        res[0] = System.currentTimeMillis() - startLoad;
        System.out.println("Load cost " + (System.currentTimeMillis() - startLoad));

        int queryOps = kvs.size() / 2;
        int kk = 0;
        long start = System.currentTimeMillis();
        while (kk < queryOps) {
            byte [] v = s.get(kvs.get(kk * 2));
            if (v == null) {
                throw new IllegalStateException("Error - " + new String(kvs.get(kk * 2)));
            }
            kk++;
        }
        fileChannel.close();
        s.close();
        res[1] = (System.currentTimeMillis() - start);
        System.out.println("End " + (System.currentTimeMillis() - start) );
        return res;
    }
}
