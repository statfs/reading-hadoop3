# pyspark  --num-executors 2  --executor-cores 1 --executor-memory 1g  --driver-memory 1g --master yarn
#pyspark  --num-executors 4  --executor-cores 2 --executor-memory 4g  --driver-memory 4g --master yarn

# Imports from pyspark 
from pyspark.context import SparkContext 
sc = SparkContext.getOrCreate() 
import sys,time

def extractor(line):
  cols = line.split(",")
  video_id = cols[0]
  date     = cols[1]
  category = cols[3]
  likes    = cols[6]
  dis_likes= cols[7]
  country  = cols[11]
  return (category, country, video_id, date, likes, dis_likes)
 
def top2(group):
  format='%y.%d.%m'
  min_tm_2 = int(time.time())
  min_tm_1 = int(time.time())
  min_v_1  = ()
  min_v_2  = ()
  for t in group:
    (category, country, video_id, date, likes, dis_likes) = t
    tm=int(time.mktime(time.strptime(date, format)))
    if tm < min_tm_1:
      min_v_1 = (category, country, video_id, likes, dis_likes)
      if min_tm_2 < min_tm_1:
        min_tm_2 = min_tm_1
        min_v_2 = min_v_1
      min_tm_1 = tm
    elif tm < min_tm_2:
      min_v_2 = (category, country, video_id, likes, dis_likes)
      min_tm_2 = tm
  if len(min_v_1) == 0 or len(min_v_2) == 0:
    return
  growth = int(min_v_2[4]) - int(min_v_1[4]) - (int(min_v_2[3]) - int(min_v_1[3]))
  return growth

def keyExchange(k,v):
  (category,country,video),growth = k,v 
  return (video+','+str(growth)+','+ category+','+country, None)

if __name__ == "__main__":
  if len(sys.argv) != 3: #0(cmd) 1(input) 2(output)
    print "need to parameter 1st: input, 2nd: output, now len(sys.argv)=", len(sys.argv)
    sys.exit(-1)
  input=sys.argv[1]
  output=sys.argv[2]
  
  #1. from file to RDD 
  raw=sc.textFile(input)
  #2. extractor useful column + ignore the header + groupby category/country/video_id + agg by date and compute growth
  all_group=raw.map(lambda line: extractor(line)).filter(lambda t:'video_id' not in t).groupBy(lambda t: (t[0],t[1],t[2])).mapValues(top2)
  #3. filter out the growth == None + get top 10
  result=all_group.filter(lambda (k,v) : v > 0).takeOrdered(10, key=lambda (k,v):-v)
  #4. exchange and format output + save to hdfs
  output_format='org.apache.hadoop.mapreduce.lib.output.TextOutputFormat'
  key_class='org.apache.hadoop.io.Text'
  value_class='org.apache.hadoop.io.NullWritable'
  sc.parallelize(result).map(lambda (k,v):keyExchange(k,v)).saveAsNewAPIHadoopFile(output, output_format, key_class, value_class)
