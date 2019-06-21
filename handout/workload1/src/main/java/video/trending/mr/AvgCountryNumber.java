package video.trending.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;

import static java.lang.Math.abs;

/**
 * @version 1.0
 * @ClassName: AvgCountryNumber
 * @CreateTime: 2019-04-05 16:31
 * @Description: TODO
 */
public class AvgCountryNumber {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.out.println("len=" + args.length + ", \n" +
                    "hadoop jar xx.jar " + AvgCountryNumber.class.toString() + " <path to input dir on hdfs>  <path to output dir on hdfs");
            return;
        }
        String input = args[0];
        String output = args[1];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName(AvgCountryNumber.class.getName());
        job.setNumReduceTasks(5);
        FileInputFormat.setInputPaths(job, input);
        job.setJarByClass(AvgCountryNumber.class);
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setPartitionerClass(CatalogPartitioner.class);
        job.setMapperClass(VideoCountryExtractor.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(AvgReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.waitForCompletion(true);

    }

    private static class VideoCountryExtractor extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length != 12 || fields[0].equals("video_id")) {
                System.out.println("parse from " + value + " failed or this line is the HEADER, fields num: " + fields.length);
                return;
            }
            //video_id,trending_date,category_id,category,publish_time,views,likes,dislikes,comment_count,ratings_disabled,video_error_or_removed,country
            String video_id = fields[0];
            String catalog = fields[3];
            String country = fields[11];
            context.write(new Text(catalog + ":#" + video_id), new Text(country));
        }
    }

    //find out each catalog how many trending lines and how many  unique video each country
    private static class AvgReducer extends Reducer<Text, Text, Text, NullWritable> {
        String curCatalog = null;
        int curVideoDistinctCnt = 0;
        int curVideoTotalCnt = 0;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("processing the catalog+video: " + key.toString());
            String[] fields = key.toString().split(":#");
            String catalog = fields[0];
            String videoId = fields[1];
            if (curCatalog == null) {
                curCatalog = catalog;
            } else if (!curCatalog.equals(catalog)) { //all the catalog previous processed
                Double avg = (double)curVideoTotalCnt/ curVideoDistinctCnt;
                System.out.println("writing the result: " + curCatalog+ ": " + avg + ", total = " + curVideoTotalCnt + ", uniq video count = " + curVideoDistinctCnt);
                context.write(new Text(curCatalog+ ": " + String.format("%.2f", avg)), NullWritable.get());
                curVideoDistinctCnt = 0;
                curVideoTotalCnt = 0;
                curCatalog = catalog;
                return;
            }
            //curCatalog just the same with this catalog
            curVideoDistinctCnt++; //different key not equal, different video_id
            HashMap<String, Integer> countryMap = new HashMap<String, Integer>();
            for (Text country : values) {
                if (countryMap.get(country.toString()) == null) {
                    countryMap.put(country.toString(), 1);
                }
            }
            curVideoTotalCnt += countryMap.size();
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Double avg = (double)curVideoTotalCnt/ curVideoDistinctCnt;
            System.out.println("writing the result: " + curCatalog + ": " + avg + ", total = " + curVideoTotalCnt + ", uniq video count = " + curVideoDistinctCnt);
            context.write(new Text(curCatalog + ": " + String.format("%.2f", avg)), NullWritable.get());
        }
    }

    private static class CatalogPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text k, Text o2, int numPartitions) {
            String[] fields = k.toString().split(":#");
            return abs(fields[0].hashCode()) % numPartitions;
        }
    }
}
