import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @version 1.0
 * @ClassName: HBaseImport
 * @CreateTime: 2019-04-04 19:44
 */
public class HBaseImport implements Tool {
    private Configuration conf;
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(conf);
        job.setMapperClass(FieldMapper.class);
        job.setReducerClass(HBaseWriter.class);
        job.setJarByClass(HBaseImport.class);
        job.setNumReduceTasks(6);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path("/tmp/rtd"));
        FileOutputFormat.setOutputPath(job, new Path("/tmp/rtd_output"));
        job.setOutputValueClass(NullWritable.class);
        return (job.waitForCompletion(true) ? 0 : -1);
    }

    public void setConf(Configuration conf) {
        this.conf = conf;

    }

    public Configuration getConf() {
        return conf;
    }

    public static class FieldMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length != 7) {
                System.out.println("bad line " + value);
                return;
            }
            String id = String.format("%07d", Integer.parseInt(fields[0]));
            String types = fields[1];
//            String dev_addr = fields[2];
            String data_name = fields[3];
            if (fields[3].equals("温度")) {
                data_name = "TEM";
            } else if (fields[3].equals("湿度")) {
                data_name = "HUM";
            } else if (fields[3].equals("照度")) {
                data_name = "LIG";
            } else if (fields[3].equals("电量")) {
                data_name = "ELC";
            }
            String datas = fields[4];
            String units = fields[5];
            String data_time = fields[6];
            Text outKey = new Text(id + "#" + data_name + "#" + data_time);
            if (units.length() == 0) {
               units = "NULL";
            }
            Text outValue = new Text((datas + "," + types + "," + units).getBytes("UTF-8"));
            context.write(outKey, outValue);
        }
    }

        // +------+-------------------------------+----------+-----------+--------+-------+---------------------+
        // | id   | types                         | dev_addr | data_name | datas  | units | data_time           |
        // +------+-------------------------------+----------+-----------+--------+-------+---------------------+
        // | 2548 | SHT21/SHT25温湿度传感器       | 4004     | 湿度      | 44.27  | %     | 2019-03-07 15:13:02 |
        // | 2549 | SHT21/SHT25温湿度传感器       | 4004     | 温度      | 15.02  | ℃     | 2019-03-07 15:13:02 |
        // | 2550 | SHT21/SHT25温湿度传感器       | 4004     | 电量      | 3.46   |       | 2019-03-07 15:13:02 |
        // | 2551 | SHT21/SHT25温湿度传感器       | 4004     | 湿度      | 118.99 | %     | 2019-03-07 15:13:02 |
        // | 2552 | SHT21/SHT25温湿度传感器       | 4004     | 温度      | 128.86 | ℃     | 2019-03-07 15:13:02 |
        // | 2553 | SHT21/SHT25温湿度传感器       | 4004     | 电量      | 3.40   |       | 2019-03-07 15:13:02 |
        // | 2554 | SHT21/SHT25温湿度传感器       | 4004     | 湿度      | 43.05  | %     | 2019-03-07 15:14:02 |
        // | 2555 | SHT21/SHT25温湿度传感器       | 4004     | 温度      | 14.74  | ℃     | 2019-03-07 15:14:02 |
        // | 2556 | SHT21/SHT25温湿度传感器       | 4004     | 电量      | 3.46   |       | 2019-03-07 15:14:02 |
        // | 2557 | SHT21/SHT25温湿度传感器       | 4004     | 湿度      | 44.41  | %     | 2019-03-07 15:15:02 |
        // +------+-------------------------------+----------+-----------+--------+-------+---------------------+

    public static class HBaseWriter extends Reducer<Text, Text, Text, NullWritable> {
        //ROW_KEY: id#data_name#data_time
        //CF: datas,types,units
        private  Table table;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Connection conf = ConnectionFactory.createConnection(context.getConfiguration());
            table = conf.getTable(TableName.valueOf("real_time_data"));
            super.setup(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Put put = new Put(key.getBytes());
            for (Text value : values) {
                String[]  columns = value.toString().split(",");
                if (columns.length != 3) {
                    System.out.println("length wrong " + key.toString() + ", " + value.toString());
                    continue;
                }
                put.addColumn("s".getBytes(), "d".getBytes(), columns[0].getBytes());
                put.addColumn("s".getBytes(), "t".getBytes(), columns[1].getBytes());
                put.addColumn("s".getBytes(), "u".getBytes(), columns[2].getBytes());
                table.put(put);
            }
            context.write(key, NullWritable.get());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            table.close();
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "11.160.46.223");
        ToolRunner.run(conf, new HBaseImport(), args);

    }
}
