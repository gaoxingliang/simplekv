package server;

import bbbb.Bitcask;
import config.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import store.KVStore;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static config.Config.MAGIC_NUMER_PUT_KEY;
import static config.Config.MAGIC_NUMER_READ_KEY;

public class Server {
    private static Logger LOG = LogManager.getLogger(Server.class);
    public static void main(String[] args) throws Exception {
        ServerSocket ss = new ServerSocket(Config.SERVER_PORT);
        //final KVStore kvStore = new SimpleBitcaskKVStore();
        final KVStore kvStore = new Bitcask();
        kvStore.open();
        LOG.info("Server started....");

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                LOG.info("Metrics {}", kvStore.metrics());
            }
        }, 1, 1, TimeUnit.SECONDS);

        ExecutorService es = Executors.newFixedThreadPool(10);
        while (true) {
            Socket t = ss.accept();
            es.submit(new SocketHandler(t, kvStore));
        }
    }


    static class SocketHandler implements Runnable{
        private final Socket s;
        private final KVStore kvStore;
        public SocketHandler(Socket s, KVStore kvStore ) {
            this.s = s;
            this.kvStore = kvStore;
        }

        @Override
        public void run() {
            LOG.info("New socket coming {}", s);
            try {
                DataInputStream dis = new DataInputStream(s.getInputStream());
                DataOutputStream dos = new DataOutputStream(s.getOutputStream());
                while (true) {
                    int op = dis.readInt();
                    if (op == Config.MAGIC_NUMER_METRIC) {
                        String metric = kvStore.metrics().toString();
                        dos.writeInt(metric.length());
                        dos.writeBytes(metric);
                        dos.flush();
                    }
                    else if (op == MAGIC_NUMER_READ_KEY) {
                        int klen = dis.readInt();
                        byte [] ks = new byte[klen];
                        dis.readFully(ks);
                        byte [] vs = kvStore.get(ks);
                        if (vs == null) {
                            dos.writeInt(0);
                        } else {
                            dos.writeInt(vs.length);
                            dos.write(vs);
                        }
                        dos.flush();
                    } else if (op == MAGIC_NUMER_PUT_KEY){
                        int klen = dis.readInt();
                        byte[] ks = new byte[klen];
                        dis.readFully(ks);
                        int vlen = dis.readInt();
                        byte[] vs = new byte[vlen];
                        dis.readFully(vs);
                        kvStore.put(ks, vs);
                    } else {
                        throw new IllegalStateException("Unknow op " + op);
                    }
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            finally {
                LOG.warn("Service accpeted client closed {}", s);
                try {
                    s.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }


}
