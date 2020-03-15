package store;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Metrics {
    public AtomicLong getTotalCount = new AtomicLong(0);
    public AtomicLong getTimeTotalInNano = new AtomicLong(0);
    public AtomicLong getMissedCount = new AtomicLong(0);
    public AtomicLong getMissedTimeTotalInNano = new AtomicLong(0);
    public AtomicLong getOkCount = new AtomicLong(0);
    public AtomicLong getOkTimeTotalInNano = new AtomicLong(0);
    public AtomicLong putCount = new AtomicLong(0);
    public AtomicLong putTimeInNano = new AtomicLong(0);

    public void flagGet(boolean found, long elapse) {
        getTotalCount.incrementAndGet();
        getTimeTotalInNano.addAndGet(elapse);
        if (found) {
            getOkCount.incrementAndGet();
            getOkTimeTotalInNano.addAndGet(elapse);
        } else {
            getMissedCount.incrementAndGet();
            getMissedTimeTotalInNano.addAndGet(elapse);
        }
    }

    public void flagPut(long elapse) {
        putCount.incrementAndGet();
        putTimeInNano.addAndGet(elapse);
    }

    @Override
    public String toString() {
        return "Metrics{" +
                "getTotalCount=" + getTotalCount +
                ", getTimeTotalAvg(ms)" + (getTotalCount.get() == 0 ? "NA" : TimeUnit.NANOSECONDS.toMillis(getTimeTotalInNano.get() /getTotalCount.get())) +
                ", getMissedCount=" + getMissedCount +
                ", getMissedTime(ms)=" + (getMissedCount.get() == 0 ? "NA" : TimeUnit.NANOSECONDS.toMillis(getMissedTimeTotalInNano.get() / getMissedCount.get())) +
                ", getOkCount=" + getOkCount +
                ", getOkTimeAvg(ms)=" + (getOkCount.get() == 0 ? "NA" :  TimeUnit.NANOSECONDS.toMillis(getOkTimeTotalInNano.get() / getOkCount.get())) +
                ", putCount=" + putCount +
                ", putTimeAvg(ms)=" + (putCount.get() == 0 ? "NA" : TimeUnit.NANOSECONDS.toMillis(putTimeInNano.get()) / putCount.get())  +
                '}';
    }
}
