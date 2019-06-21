package video.trending.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * @version 1.0
 * @ClassName: ControvTrend
 * @CreateTime: 2019-04-05 17:34
 * @Description: TODO
 */
public class ControversialTrend {
    public static class Trend implements Serializable {
        private String videoId;
        private String trendingDate;
        private int categoryId;
        private String category;
        private String publishTime;
        private long views;
        private long likes;
        private long dislikes;
        private long commentCount;
        private boolean ratingsDisabled;
        private boolean videoErrorOrRemoved;
        private String country;

        public String getVideoId() {
            return videoId;
        }

        public void setVideoId(String videoId) {
            this.videoId = videoId;
        }

        public String getTrendingDate() {
            return trendingDate;
        }

        public void setTrendingDate(String trendingDate) {
            this.trendingDate = trendingDate;
        }

        public int getCategoryId() {
            return categoryId;
        }

        public void setCategoryId(int categoryId) {
            this.categoryId = categoryId;
        }

        public String getCategory() {
            return category;
        }

        public void setCategory(String category) {
            this.category = category;
        }

        public String getPublishTime() {
            return publishTime;
        }

        public void setPublishTime(String publishTime) {
            this.publishTime = publishTime;
        }

        public long getViews() {
            return views;
        }

        public void setViews(long views) {
            this.views = views;
        }

        public long getLikes() {
            return likes;
        }

        public void setLikes(long likes) {
            this.likes = likes;
        }

        public long getDislikes() {
            return dislikes;
        }

        public void setDislikes(long dislikes) {
            this.dislikes = dislikes;
        }

        public long getCommentCount() {
            return commentCount;
        }

        public void setCommentCount(long commentCount) {
            this.commentCount = commentCount;
        }

        public boolean getRatingsDisabled() {
            return ratingsDisabled;
        }

        public void setRatingsDisabled(boolean ratingsDisabled) {
            this.ratingsDisabled = ratingsDisabled;
        }

        public boolean getVideoErrorOrRemoved() {
            return videoErrorOrRemoved;
        }

        public void setVideoErrorOrRemoved(boolean videoErrorOrRemoved) {
            this.videoErrorOrRemoved = videoErrorOrRemoved;
        }

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("len=" + args.length + ", \n" +
                    "spark-submit xx.jar " + ControversialTrend.class.toString() + " <path to input dir on hdfs>  <path to output dir on hdfs");
            return;
        }
        String input = args[0];
        String output = args[1];
        SparkConf sparkConf = new SparkConf().setAppName(ControversialTrend.class.toString());
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sc.textFile(input);
//        JavaRDD<Trend> records = lines.map(line -> {
//            String[] fields = line.split(",");
//            Trend trend = new Trend();
//            //video_id,trending_date,category_id,category,publish_time,views,likes,dislikes,comment_count,ratings_disabled,video_error_or_removed,country
//            trend.setVideoId(fields[0]);
//            trend.setTrendingDate(fields[1]);
//            trend.setCategoryId(Integer.parseInt(fields[2]));
//            trend.setCategory(fields[3]);
//            trend.setPublishTime(fields[4]);
//            trend.setViews(Long.parseLong(fields[5]));
//            trend.setLikes(Long.parseLong(fields[6]));
//            trend.setDislikes(Long.parseLong(fields[7]));
//            trend.setCommentCount(Long.parseLong(fields[8]));
//            trend.setRatingsDisabled(Boolean.parseBoolean(fields[9]));
//            trend.setVideoErrorOrRemoved(Boolean.parseBoolean(fields[10]));
//            trend.setCountry(fields[11]);
//            return trend;
//        });
//        Dataset<Row> df = sparkSession.createDataFrame(records, Trend.class);
//        df.select("videoId", "catalog", "country", "likes", "dislikes", "trending_date").groupBy("videoId");
        //TODO

    }
}
