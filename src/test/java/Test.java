import bitcask.Bitcask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import store.KVStore;

public class Test {
    private static Logger LOG = LogManager.getLogger(Bitcask.class);

    public static void main(String[] args) throws Exception{
        KVStore k = new Bitcask();
        k.open();
        //k.put("hello".getBytes(), "world".getBytes());
        System.out.println(new String(k.get("hello".getBytes())));
        System.out.println(k.get("hello2".getBytes()));
        k.close();
    }
}
