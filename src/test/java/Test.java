import bitcask.Bitcask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import store.KVStore;

import java.util.ArrayList;
import java.util.List;

public class Test {
    private static Logger LOG = LogManager.getLogger(Bitcask.class);

    public static void main(String[] args) throws Exception{
        KVStore k = new Bitcask();
        k.open();

        int randoms = 100000;
        List<String> randomList = new ArrayList<>(randoms);
        for (int i = 0; i < randoms; i++) {
        }


        k.close();
    }

}
