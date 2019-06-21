package category.trending.correlation.spark;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * 2019-04-08 12:51
 */
public class Main {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("spark-submit jar this.jar category.trending.correlation.spark.Main input ouput");
            return;
        }
        String in = args[0];
        String out = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("category.trending.correlation.spark");
//        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sc.textFile(in);
        //1. extractor fields and filter bad lines
        JavaRDD<String> records = lines.map(line -> {
            String[] fields = line.split(",");
            String video = fields[0];
            if (video.equals("video_id")) {
                return null;
            }
            String date = fields[1];
            String category = fields[3];
            String likes = fields[6];
            String dislikes = fields[7];
            String country = fields[11];
            return String.join(",", video, category, country, likes, dislikes, date);
        }).filter(line -> line != null);
//        System.out.println(lines.count() + "vs." + records.count());

        //2. rdd to pair rdd kv=(video+category+country, likes+dislikes+date)
        JavaPairRDD<String, String> pair = records.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] fields = s.split(",");
                String video = fields[0];
                String category = fields[1];
                String country = fields[2];
                String likes = fields[3];
                String dislikes = fields[4];
                String date = fields[5];
                return new Tuple2<String, String>(String.join(",", video, category, country),
                        String.join(",", likes, dislikes, date));
            }
        });

        //3. groupby video+category+country, filter out first and second record according to the date value, calc growth
        JavaPairRDD<String, Iterable<String>> grouped = pair.groupByKey();
        JavaPairRDD<Integer, String> growth = grouped.mapToPair(new PairFunction<Tuple2<String,Iterable<String>>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
                Iterator<String> records = tuple._2.iterator();
                SimpleDateFormat format = new SimpleDateFormat("yy.dd.MM");
                Date first = new Date();
                Date second = new Date();
                String[] minValue = new String[2]; //0 first, 2 second
                int i = 0;
                while (records.hasNext()) {
                    i++;
                    String cur = records.next();
                    String date = cur.split(",")[2];
                    Date datetime = format.parse(date);
                    if (tuple._1.contains("ZGEoqPpJQLE")) {
                        System.out.println(i + "," + tuple._1 + " <= cur|" + cur + " | first = " + first + ", second= " + second + ", this=" + datetime);
                    }
                    if (datetime.before(first)) {
                        minValue[0] = cur;
                        if (!second.after(first)) {
                            second = first;
                            minValue[1] = minValue[0];
                        }
                        first = datetime;
                    } else if (datetime.before(second)) {
                        minValue[1] = cur;
                        second = datetime;
                    }
                }
                if (i < 2) {
                    return null;
                }
                if (minValue[0] == null || minValue[1] == null) {
                    return  null;
                }
                int firstLike = Integer.parseInt(minValue[0].split(",")[0]);
                int firstDislike = Integer.parseInt(minValue[0].split(",")[1]);
                int secondLike = Integer.parseInt(minValue[1].split(",")[0]);
                int secondDislike = Integer.parseInt(minValue[1].split(",")[1]);
                int growth = (secondDislike - firstDislike) - (secondLike - firstLike);
                if (tuple._1.contains("ZGEoqPpJQLE")) {
                    System.out.println("debug:" + minValue[0] + "|" + minValue[1] + "|" + growth);
                }
                return new Tuple2<Integer, String>(growth, tuple._1);
            }
        });

//        sort by growth and get top 10
        List<Tuple2<Integer, String>> ret = growth.filter(t -> t != null).sortByKey(false).take(10);
//        for (Tuple2<Integer, String> v : ret) {
//            System.out.println(v._2 + "," + v._1);
//        }

        //output
        sc.parallelizePairs(ret).mapToPair(tuple -> {
            String v = String.valueOf(tuple._1);
            String[] f = tuple._2.split(",");
            return new Tuple2<>(new Text(String.join(",", f[0], v, f[1], f[2])), NullWritable.get());
        }).saveAsHadoopFile(out, Text.class, NullWritable.class, org.apache.hadoop.mapred.TextOutputFormat.class);

    }
}
