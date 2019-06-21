package upo.study.hdfs.ec;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author wenchao
 * @version 1.0
 * @ClassName: MaxLengthReducer
 * @CreateTime: 2019-03-21 18:14
 * @Description: TODO
 */
public class MaxLengthReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String fileName = key.toString();
        long maxLen = 0;
        long minLen = Integer.MAX_VALUE;
        String loc = "";
        for (Text v : values) {
            String[] vStr = v.toString().split(":");
            long len = Long.parseLong(vStr[2]);
            if (len < minLen ) {
                minLen = len;
            }
            if (len > maxLen) {
                maxLen = len;
                loc = vStr[3];
            }
        }
        context.write(new  Text(fileName), new Text(loc + ":" + maxLen + ":" + minLen));
    }
}
