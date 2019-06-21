package upo.study.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by upo on 2019/4/8.
 */
public class RDDTest {
    public static void main(String[] args) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yy.dd.MM");
        Date d = format.parse("17.28.02");
        System.out.println(d.before(new Date()));
        String master = "local[2]";
        SparkConf conf = new SparkConf().setAppName(RDDTest.class.toString()).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("/home/statfs/Downloads/AllVideos_short.csv");
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
        System.out.println(lines.count() + "vs." + records.count());
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
        JavaPairRDD<String, Iterable<String>> grouped = pair.groupByKey();
        System.out.println("GB: " + pair.count() + "vs." + grouped.count());
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
//                        if (!second.after(first)) {
                            second = first;
                            minValue[1] = minValue[0];
                        }
                        first = datetime;
                    } else if (datetime.before(second)) {
                        minValue[1] = cur;
                        second = datetime;
                    }
                }
//                System.out.println("debug:" + minValue[0] +" vs. " + minValue[1] );
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

//        growth.collect();
//        System.out.println(growth.count());
        List<Tuple2<Integer, String>> ret = growth.filter(t -> t != null).sortByKey(false).take(10);
//        growth.filter(t -> t != null).take(10);
        for (Tuple2<Integer, String> v : ret) {
            System.out.println(v._2 + "," + v._1);
        }

//        demo(lines);

    }

    private static void demo(JavaRDD<String> lines) {
        JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
        int totalLength = lineLengths.reduce((a, b) -> a + b);
        System.out.println(totalLength);

        JavaRDD<Integer> lineLengths2 = lines.map(new Function<String, Integer>() {
            public Integer call(String s) { return s.length(); }
        });
        int totalLength2 = lineLengths2.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) { return a + b; }
        });
        System.out.println(totalLength2);
    }
}
