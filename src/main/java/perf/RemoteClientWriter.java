package perf;

import config.Config;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

public class RemoteClientWriter {

    private static Logger LOG = LogManager.getLogger(RemoteClientWriter.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        /**
         * load another test file
         */
        int counts [] = new int[] {10000, 10000, 1000000};
        int writer = 10;
        ExecutorService es = Executors.newFixedThreadPool(writer);
        Executors.newSingleThreadExecutor().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    metricsPrinter();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        for (int c : counts) {
            final int x = c;
            es.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        send2ServerWithTest(x);
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

    private static void metricsPrinter() throws IOException {
        Socket s = new Socket();
        s.connect(new InetSocketAddress("localhost", Config.SERVER_PORT));
        System.out.println("Metrics client started....");
        DataOutputStream out = new DataOutputStream(s.getOutputStream());
        DataInputStream in = new DataInputStream(s.getInputStream());
        while (true) {

            try {
                out.writeInt(Config.MAGIC_NUMER_METRIC);
                int mlen = in.readInt();
                byte [] mv = new byte[mlen];
                in.readFully(mv);
                LOG.info("metrics {}", new String(mv));
                Thread.sleep(1000);
            }
            catch (Exception e) {
                IOUtils.closeQuietly(in, out, s);
                break;
            }
        }
    }

    private static void send2ServerWithTest(int keyCount) throws IOException, InterruptedException {
        Socket s = new Socket();
        s.connect(new InetSocketAddress("localhost", Config.SERVER_PORT));
        System.out.println("Client started....");
        DataOutputStream out = new DataOutputStream(s.getOutputStream());

        File newFile = Generator.generatePerfTestFile(keyCount);
        DataInputStream in = new DataInputStream(new FileInputStream(newFile));
        int pos = 0;
        while (pos < newFile.length()) {
            int klen = in.readInt();
            byte [] ks = new byte[klen];
            in.readFully(ks);
            int vlen = in.readInt();
            byte [] vs = new byte[vlen];
            in.readFully(vs);
            out.writeInt(Config.MAGIC_NUMER_PUT_KEY);
            out.writeInt(klen);
            out.write(ks);
            out.writeInt(vlen);
            out.write(vs);
            out.flush();
            pos += 8 + klen + vlen;

            Thread.sleep(100 + new Random().nextInt(100));
        }
        in.close();
        System.out.println("Client finished....");
    }
}
