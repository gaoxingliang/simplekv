package perf;

import config.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import store.KVStore;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static perf.Generator.newRandomChars;

public class RemoteClientReader {

    private static Logger LOG = LogManager.getLogger(RemoteClientReader.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        /**
         * load another test file
         */
        int reader = 1;
        int singleReadCount = 10000;
        ExecutorService es = Executors.newFixedThreadPool(reader);

        for (int i = 0; i <reader; i++) {
            final int x = i;
            es.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        send2ServerWithTestGet(singleReadCount, new File("perftestFile1000000"));
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        es.shutdown();
        es.awaitTermination(5, TimeUnit.MINUTES);
        es.shutdownNow();

    }


    private static void send2ServerWithTestGet(int readCount, File inputFile) throws IOException, InterruptedException {
        Socket s = new Socket();
        s.connect(new InetSocketAddress("localhost", Config.SERVER_PORT));
        System.out.println("read Client started....");
        DataOutputStream socketOut = new DataOutputStream(s.getOutputStream());
        DataInputStream socketIn = new DataInputStream(s.getInputStream());
        Random r = new Random();
        boolean shouldGot = false;
        DataInputStream fis = new DataInputStream(new FileInputStream(inputFile));

        while (readCount -- >= 0) {
            LOG.info("start reading {}", readCount);
            byte [] bs = null;
            if (r.nextInt(10) < 2) {
                shouldGot = true;
                int klen = fis.readInt();
                byte ks [] = new byte[klen];
                fis.readFully(ks);
                bs = ks;
                int vlen = fis.readInt();
                fis.skipBytes(vlen);
                LOG.info("reading key len: {}", klen);
            }
            else {
                shouldGot = false;
                bs = newRandomChars(Math.max(5, r.nextInt(KVStore.MAX_KEY_LEN)), 0, 26);
            }
            LOG.info("Readingg key ={}", new String(bs));
            socketOut.writeInt(Config.MAGIC_NUMER_READ_KEY);
            socketOut.writeInt(bs.length);
            socketOut.write(bs);
            socketOut.flush();
            int vlen = socketIn.readInt();
            if (vlen == 0) {
                // not found
                if (shouldGot) {
                    throw new IllegalStateException("Something error for key -" + new String(bs));
                }
            } else {
                byte [] vs = new byte[vlen];
                socketIn.readFully(vs);
            }
            Thread.sleep(10 + r.nextInt(10));
        }
    }
}
