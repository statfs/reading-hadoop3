import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @version 1.0
 * @ClassName: AvgRevenue
 * @CreateTime: 2019-06-18 18:00
 */
public class AvgRevenue {
    private static class RevenueMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] val = value.toString().split(",");
            if (val.length < 9) {
                return;
            }
            String month = val[1];
            String device = val[3];
            try {
                Double revenue = Double.parseDouble(val[4]);
                context.write(new Text(month + "\t" + device), new DoubleWritable(revenue));
            } catch (NumberFormatException e) {
                return;
            }
        }
    }

    private static class AvgReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            double sum = 0;
            for (DoubleWritable v : values) {
                count++;
                sum += v.get();
            }
            context.write(key, new DoubleWritable(sum / count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.out.println("hadoop jar a.jar in out");
            return;
        }

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        String in = args[0];
        String out = args[1];
//        Integer num = Integer.parseInt(args[2]);
        job.setMapperClass(RevenueMapper.class);
        TextInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setReducerClass(AvgReducer.class);
        job.setNumReduceTasks(1);
        job.setJarByClass(AvgRevenue.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
