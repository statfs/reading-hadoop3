package upo.study.hdfs.ec;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author wenchao
 * @version 1.0
 * @ClassName: CorruptBlockLocator
 * @CreateTime: 2019-03-21 18:03
 * @Description: TODO
 */
public class CorruptBlockLocator {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.exit(-1);
        }
        String in = args[0];
        String out = args[1];
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        Job job = Job.getInstance(conf);
        FileInputFormat.addInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.getConfiguration().set("mapred.min.split.size", String.valueOf(256*1024*1024));
        job.setMapperClass(AverageLengthMapper.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setJarByClass(CorruptBlockLocator.class);
        job.setReducerClass(MaxLengthReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(2);
        job.waitForCompletion(true);
    }
}
