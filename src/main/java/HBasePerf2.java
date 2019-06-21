import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @version 1.0
 * @ClassName: HBasePerf2
 * @CreateTime: 2019-04-05 14:25
 */
public class HBasePerf2 {
    private static final int reqNum = 100000;
    private static final int parallel = 8;

    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "11.160.46.223");
        configuration.set("fs.defaultFS", "hdfs://11.160.46.223");
        String[] tables = new String[]{"t10w", "t100w", "t1000w"};
        for (String table : tables) {
            mockTest(configuration, table);
        }
        perfTest(configuration);

    }

    private static void mockTest(final Configuration configuration, final String table) throws InterruptedException {
        long first = System.currentTimeMillis();
        final int batch = reqNum / parallel;
        ExecutorService service = new ThreadPoolExecutor(parallel, parallel, 5, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(parallel));
        for (int i = 0; i < parallel; ++i) {
            service.execute(new Runnable() {
                public void run() {
                    try {
                        Connection conn = ConnectionFactory.createConnection(configuration);
                        final Table htable = conn.getTable(TableName.valueOf(table));
                        long start = System.currentTimeMillis();
                        int id = (int) Thread.currentThread().getId() % parallel;
                        for (int i = id * batch; i < (id + 1) * batch; ++i) {
                            int prefix = 65 + (i / 250000);
                            if (prefix >= 90) {
                                prefix += 7;
                            }
                            String rowkey = (char) prefix + String.format("%08d", (i + 1));
                            Get get = new Get(Bytes.toBytes(rowkey));
                            get.addColumn("f".getBytes(), "c".getBytes());
                            Result res = htable.get(get);
                            byte[] result = res.getValue("f".getBytes(), "c".getBytes());
                            if (result == null) {
                                System.out.println(rowkey + " with no result");
                                continue;
                            }
                        }
                        long end = System.currentTimeMillis();
                        System.out.println("access table " + table + " via thread = " + Thread.currentThread().getId() + ", idx=" + id + ", complete req from " + id * batch + " to " + (id + 1) * batch + ", time cost(s): " + (end - start) / 1000.0);
                        conn.close();
                    } catch (IOException e) {
                        System.out.println("read from hbase failed " + e.getMessage());
                    }
                }
            });
        }
        service.shutdownNow();
        service.awaitTermination(10, TimeUnit.SECONDS);
        long last = System.currentTimeMillis();
        System.out.println("complete req = " + reqNum + ", time cost(s): " + (last - first) / 1000.0);
    }

    private static void perfTest(final Configuration configuration) throws IOException, InterruptedException {
        final String table = "real_time_data";
        FileSystem fileSystem = FileSystem.get(configuration);
        FSDataInputStream inputStream = fileSystem.open(new Path("/tmp/rtd_output_10w"));
        int max = 100000;
        final int batch = reqNum / parallel;
        final String[] buffer = new String[max];
        for (int i = 0; i < max; ++i) {
            buffer[i] = inputStream.readLine();
        }
        ExecutorService service = new ThreadPoolExecutor(parallel, parallel, 5, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(parallel));
        long first = System.currentTimeMillis();
        for (int i = 0; i < parallel; ++i) {
            service.execute(new Runnable() {
                public void run() {
                    try {
                        Connection conn = ConnectionFactory.createConnection(configuration);
                        final Table htable = conn.getTable(TableName.valueOf(table));
                        long start = System.currentTimeMillis();
                        int id = (int) Thread.currentThread().getId() % parallel;
                        for (int i = id * batch; i < (id + 1) * batch; ++i) {
                            Get get = new Get(Bytes.toBytes(buffer[i]));
                            get.addColumn("s".getBytes(), "d".getBytes());
                            Result res = htable.get(get);
                            byte[] result = res.getValue("s".getBytes(), "d".getBytes());
                            if (result == null) {
                                System.out.println(buffer[i] + " with no result");
                                continue;
                            }
                        }
                        long end = System.currentTimeMillis();
                        System.out.println("access table via thread = " + Thread.currentThread().getId() + ", idx=" + id + ", complete req from " + id * batch + " to " + (id + 1) * batch + ", time cost(s): " + (end - start) / 1000.0);
                        conn.close();
                    } catch (IOException e) {
                        System.out.println("read from hbase failed " + e.getMessage());
                    }
                }
            });
        }
        service.shutdownNow();
        service.awaitTermination(10, TimeUnit.SECONDS);
        long last = System.currentTimeMillis();
        System.out.println("complete req = " + reqNum + ", time cost(s): " + (last - first) / 1000.0);
    }
}
