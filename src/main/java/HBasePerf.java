import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @version 1.0
 * @ClassName: HBasePerf
 * @CreateTime: 2019-04-04 18:25
 */
public class HBasePerf {
    private static int[] reqNums = new int[]{1, 1000, 100000};
    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "11.160.46.223");
        configuration.set("fs.defaultFS", "hdfs://11.160.46.223");
        Connection conn = ConnectionFactory.createConnection(configuration);
        mockTest(conn);
        perfTest(conn);
        conn.close();

    }

    private static void perfTest(Connection conn) throws IOException {
        String table = "real_time_data";
        Table htable = conn.getTable(TableName.valueOf(table));
        FileSystem fileSystem = FileSystem.get(conn.getConfiguration());
        FSDataInputStream inputStream = fileSystem.open(new Path("/tmp/rtd_output_10w"));
        int max = 100000;
        String[] buffer = new String[max];
        for (int i = 0; i < max; ++i) {
            buffer[i] = inputStream.readLine();
        }
        for (int reqNum : reqNums) {
            long start = System.currentTimeMillis();
            for (int i = 0; i < reqNum; ++i) {
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
            System.out.println("access table = " + table + ", complete req = " + reqNum + ", time cost(s): " + (end - start)/1000.0);
        }

    }

    private static void mockTest(Connection conn) throws IOException {
        String[] tables = new String[] {"t10w", "t100w", "t1000w"};
        for (String table : tables) {
            Table htable = conn.getTable(TableName.valueOf(table));
            for (int reqNum : reqNums) {
                long start = System.currentTimeMillis();
                for (int i = 0; i < reqNum; ++i) {
                    int prefix = 65 + (i / 250000);
                    if (prefix >= 90) {
                        prefix += 7;
                    }
                    String rowkey = (char)prefix + String.format("%08d", (i + 1));
                    Get get = new Get(Bytes.toBytes(rowkey));
                    get.addColumn("f".getBytes(), "c".getBytes());
                    Result res = htable.get(get);
                    byte[] result = res.getValue("f".getBytes(), "c".getBytes());
                    if (result == null) {
                        System.out.println(rowkey + " with no result");
                        continue;
                    }
                    //System.out.println(Bytes.toString(result));
                }
                long end = System.currentTimeMillis();
                System.out.println("access table = " + table + ", complete req = " + reqNum + ", time cost(s): " + (end - start)/1000.0);
            }
        }
    }
}
