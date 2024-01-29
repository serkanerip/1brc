package dev.morling.onebrc;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class CalculateAverage_serkanerip {

    private static final String FILE = "measurements.txt";

    private static Map<String, Double> doubles;

    private static final int THREAD_COUNT = 8;

    private static class Station {
        long count;

        double min, max, sum;

        public Station(long count, double min, double max, double sum) {
            this.count = count;
            this.min = min;
            this.max = max;
            this.sum = sum;
        }

        public Station add(Station station) {
            if (station == null) {
                return this;
            }
            count += station.count;
            min = station.min > min ? min : station.min;
            max = station.max > max ? station.max : max;
            sum += station.sum;
            return this;
        }

        @Override
        public String toString() {
            return "%.1f/%.1f/%.1f".formatted(min, sum / count, max);
        }
    }

    private static class Worker implements Runnable {

        private final MappedByteBuffer mbb;

        private final Consumer<Map<String, Station>> resultConsumer;

        Map<String, Station> hm;

        private Worker(MappedByteBuffer buffer, Consumer<Map<String, Station>> c) {
            this.mbb = buffer;
            this.hm = new HashMap<>(10000, 200);
            this.resultConsumer = c;
        }

        @Override
        public void run() {
            byte[] strBuffer = new byte[200];
            int length = 0, commaIndex = 0;
            byte c;
            long remaining = mbb.limit();
            while (remaining != 0) {
                remaining--;
                c = mbb.get();
                if (c != '\n') {
                    if (c == ';') {
                        commaIndex = length;
                    }
                    strBuffer[length++] = c;
                    continue;
                }
                var read_k = new String(strBuffer, 0, commaIndex);
                var read_v = new String(strBuffer, commaIndex + 1, length - commaIndex - 1);
                hm.compute(read_k, (k, v) -> {
                    double degree = doubles.get(read_v);
                    if (v == null) {
                        return new Station(1, degree, degree, degree);
                    }
                    v.count++;
                    v.sum += degree;
                    if (degree > v.max) {
                        v.max = degree;
                    }
                    if (degree < v.min) {
                        v.min = degree;
                    }
                    return v;
                });
                length = 0;
            }
            resultConsumer.accept(hm);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        doubles = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 10; j++) {
                var key = "%d.%d".formatted(i, j);
                doubles.put(key, Double.parseDouble(key));
                key = "-%d.%d".formatted(i, j);
                doubles.put(key, Double.parseDouble(key));
            }
        }
        var finalHm = new TreeMap<String, Station>(Comparator.naturalOrder());
        var lock = new ReentrantLock();
        RandomAccessFile file = new RandomAccessFile(FILE, "r");
        FileChannel channel = file.getChannel();
        var es = Executors.newFixedThreadPool(THREAD_COUNT);
        var sizePer = channel.size() / THREAD_COUNT;
        var futures = new ArrayList<Future>();
        var positions = new long[THREAD_COUNT];
        var buf = new byte[120];
        for (int i = 0; i < THREAD_COUNT; i++) {
            if (i == 0) {
                positions[i] = 0;
                continue;
            }
            var pos = sizePer * i;
            file.seek(pos);
            file.read(buf);
            for (int j = 0; j < buf.length; j++) {
                if (buf[j] == '\n') {
                    positions[i] = pos + j + 1;
                    break;
                }
            }
        }
        file.seek(0);
        long remainingSize = channel.size();
        for (int i = 0; i < THREAD_COUNT; i++) {
            var pos = positions[i];
            var size = i == THREAD_COUNT - 1 ? remainingSize : positions[i + 1] - positions[i];
            remainingSize -= size;
            futures.add(es.submit(new Worker(
                    channel.map(FileChannel.MapMode.READ_ONLY, pos, size),
                    m -> {
                        lock.lock();
                        m.forEach((mk, mv) -> finalHm.compute(mk, (k, v) -> mv.add(v)));
                        lock.unlock();
                    })));
        }
        for (Future future : futures) {
            future.get();
        }
        System.out.println(finalHm);
        es.shutdown();
        channel.close();
        file.close();
    }
}
