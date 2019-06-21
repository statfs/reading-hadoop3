package upo.study.hdfs.ec;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author wenchao
 * @version 1.0
 * @ClassName: AverageLengthMapper
 * @CreateTime: 2019-03-21 18:04
 * @Description: TODO
 */
public class AverageLengthMapper extends Mapper<LongWritable, Text, Text, Text> {
    private String fileName = "";
    private long recordCounter = 0;
    private long lenCounter  = 0;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //InputSplit inputSplit = context.getInputSplit();
        //FileSplit split = (FileSplit) context.getInputSplit();
        //fileName = split.getPath().getName();
        //String[] locs = split.getLocations();
        //StringBuilder sb = new StringBuilder();
        //for (String loc : locs) {
        //    System.out.println("loc: " + loc);
        //    sb.append(","+loc);
        //}
        //SplitLocationInfo[] splitLocationInfos =  split.getLocationInfo();
        //if (splitLocationInfos != null) {
        //    for (SplitLocationInfo splitLocationInfo : splitLocationInfos) {
        //        System.out.println("splitLoc: " + splitLocationInfo);
        //        sb.append("," + splitLocationInfo);
        //    }
        //}
        //fileName+=sb.toString();
        //super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        lenCounter+=line.length();
        recordCounter++;

        //TODO cannot get split location, why?
        FileSplit split = (FileSplit) context.getInputSplit();
        fileName = split.getPath().getName();
        fileName += "," + split.getStart();
        fileName += "," + split.getLength();
        String[] locs = split.getLocations();
        StringBuilder sb = new StringBuilder();
        for (String loc : locs) {
            System.out.println("loc: " + loc);
            sb.append(","+loc);
        }
        SplitLocationInfo[] splitLocationInfos =  split.getLocationInfo();
        if (splitLocationInfos != null) {
            for (SplitLocationInfo splitLocationInfo : splitLocationInfos) {
                System.out.println("splitLoc: " + splitLocationInfo);
                sb.append("," + splitLocationInfo);
            }
        }
        fileName+=sb.toString();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String[] tmp = fileName.split(",");
        String v = lenCounter + ":" + recordCounter + ":" + (lenCounter/recordCounter) + ":" + fileName + ":" + tmp[1] + ":" + tmp[2] ;
        context.write(new Text(tmp[0]), new Text(v));
    }
}
