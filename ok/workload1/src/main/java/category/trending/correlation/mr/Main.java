package category.trending.correlation.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * 2019-04-08 10:10
 */
public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length != 2) {
            System.err.println("hadoop jar this.jar category.trending.correlation.mr.Main input ouput");
            return;
        }
        String in = args[0];
        String out = args[1];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("category trending correlation");
        job.setNumReduceTasks(2);
        FileInputFormat.setInputPaths(job, in);
        job.setJarByClass(Main.class);
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.setPartitionerClass(PartitionByCategoryName.class);
        job.setMapperClass(ParserTrendingMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(TrendingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : -1);

    }

    private static class PartitionByCategoryName extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable integer, int numPartitions) {
            String category = key.toString().split("\\|")[0];
            return category.length() % numPartitions;
        }
    }

    private static class ParserTrendingMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line[] = value.toString().split(",");
            if (line.length == 12 && !line[0].equals("video_id")) {
                String newKey = line[3] + "|" + line[0] + "|" + line[11];
                System.out.println("writing: " + newKey);
                context.write(new Text(newKey), new IntWritable(1));
            }
        }
    }

    private static class TrendingReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        private String lastCategory = "";
        private String lastVideo = "";
        private String lastCountry = "";
        private int videoCounter = 0;
        private int countryCounter = 0;
        private DecimalFormat df = new DecimalFormat("#.00");
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println("processing: " + key);
            String[] triple = key.toString().split("\\|");
            String category = triple[0];
            String video = triple[1];
            String country = triple[2];
            if (lastCategory.length() == 0 && lastCountry.length()  ==  0 && lastVideo.length() == 0) {
                lastCountry = country;
                lastVideo = video;
                lastCategory = category;
                videoCounter = 1;
                countryCounter = 1;
            } else if (!lastCategory.equals(category)) {
                System.out.println("writing: " + lastCategory);
                Double v = Double.valueOf(df.format((double)countryCounter/videoCounter));
                context.write(new Text(lastCategory), new DoubleWritable(v));
                videoCounter = 1;
                countryCounter = 1;
                lastCategory = category;
                lastVideo = video;
                lastCountry = country;
                return;
            } else if (!lastVideo.equals(video)) {
                videoCounter++;
                lastVideo = video;
            }
            countryCounter++;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (countryCounter == 0) {
                return;
            }
            System.out.println("writing: " + lastCategory);
            Double v = Double.valueOf(df.format((double)countryCounter/videoCounter));
            context.write(new Text(lastCategory), new DoubleWritable(v));
        }
    }
}
