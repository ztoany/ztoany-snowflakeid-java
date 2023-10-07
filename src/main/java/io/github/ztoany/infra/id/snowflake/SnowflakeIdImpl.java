package io.github.ztoany.infra.id.snowflake;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * implement twitter snowflake id
 *
 */
public class SnowflakeIdImpl {
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    /**
     *
     * 2023-01-01
     *
     */
    private static final long EPOCH = 1672531200000L;


    private static final long SEQUENCE_BITS = 12;
    private static final long WORKER_ID_BITS = 5;
    private static final long DATACENTER_ID_BITS = 5;


    private static final long TIMESTAMP_BITS = 41L;
    private static final long UNUSED_BITS = 1L;

    private static final long MAX_SEQUENCE = -1 ^ (-1L << SEQUENCE_BITS);
    private static final long MAX_WORKER_ID = -1 ^ (-1L << WORKER_ID_BITS);
    private static final long MAX_DATACENTER_ID = -1 ^ (-1L << DATACENTER_ID_BITS);


    private static final long WORKER_ID_SHIFT = SEQUENCE_BITS;
    private static final long DATACENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;
    private static final long TIMESTAMP_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATACENTER_ID_BITS;


    private final long datacenterId;

    private final long workerId;

    private long sequence = 0L;

    private long lastTimestamp = -1L;


    private final ReentrantLock lock = new ReentrantLock();

    public SnowflakeIdImpl(long datacenterId, long workerId) {
        if (datacenterId > MAX_DATACENTER_ID || datacenterId < 0) {
            throw new IllegalArgumentException(
                    "datacenterId can't be greater than MAX_DATACENTER_ID or less than 0");
        }
        if (workerId > MAX_WORKER_ID || workerId < 0) {
            throw new IllegalArgumentException(
                    "workerId can't be greater than MAX_WORKER_ID or less than 0");
        }
        this.datacenterId = datacenterId;
        this.workerId = workerId;
    }

    public long nextId() {
        lock.lock();
        try {
            long currTimestamp = timestampGen();
            if (currTimestamp < lastTimestamp) {
                throw new IllegalStateException(String.format("Clock moved backwards. Refusing to generate id for %d milliseconds", lastTimestamp - currTimestamp));
            }

            if (currTimestamp == lastTimestamp) {
                sequence = (sequence + 1) & MAX_SEQUENCE;
                if (sequence == 0L) {
                    currTimestamp = tilNextMillis();
                }
            } else {
                sequence = 0L;
            }

            lastTimestamp = currTimestamp;

            return ((currTimestamp - EPOCH) << TIMESTAMP_SHIFT)
                    | (datacenterId << DATACENTER_ID_SHIFT)
                    | (workerId << WORKER_ID_SHIFT)
                    | sequence;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        return "SnowflakeId [timestampBits=" + TIMESTAMP_BITS + ", datacenterIdBits=" + DATACENTER_ID_BITS
                + ", workerIdBits=" + WORKER_ID_BITS + ", sequenceBits=" + SEQUENCE_BITS + ", epoch=" + EPOCH
                + ", datacenterId=" + datacenterId + ", workerId=" + workerId + "]";
    }

    public static long getEpoch() {
        return EPOCH;
    }

    /**
     *
     * [timestamp, datacenterId, workerId, sequence, timestamp - epoch]
     *
     */
    public static long[] parseId(long id) {
        long[] arr = new long[5];
        arr[4] = ((id & diode(UNUSED_BITS, TIMESTAMP_BITS)) >> TIMESTAMP_SHIFT);
        arr[0] = arr[4] + EPOCH;
        arr[1] = (id & diode(UNUSED_BITS + TIMESTAMP_BITS, DATACENTER_ID_BITS)) >> DATACENTER_ID_SHIFT;
        arr[2] = (id & diode(UNUSED_BITS + TIMESTAMP_BITS + DATACENTER_ID_BITS, WORKER_ID_BITS)) >> WORKER_ID_SHIFT;
        arr[3] = (id & diode(UNUSED_BITS + TIMESTAMP_BITS + DATACENTER_ID_BITS + WORKER_ID_BITS, SEQUENCE_BITS));
        return arr;
    }

    /**
     *
     * utcDateTime, #sequence, (datacenterId, workerId)
     *
     */
    public static String formatId(long id) {
        long[] arr = parseId(id);
        ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(arr[0]), ZoneOffset.UTC);
        String timeStr = dtf.format(zdt);
        return String.format("%s, #%d, (%d,%d)", timeStr, arr[3], arr[1], arr[2]);
    }

    private long timestampGen() {
        return System.currentTimeMillis();
    }

    private long tilNextMillis() {
        long timestamp = timestampGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timestampGen();
        }
        return timestamp;
    }

    private static long diode(long offset, long length) {
        int lb = (int) (64 - offset);
        int rb = (int) (64 - (offset + length));
        return (-1L << lb) ^ (-1L << rb);
    }
}
