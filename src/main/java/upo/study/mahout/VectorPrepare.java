package upo.study.mahout;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorIterable;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.utils.vectors.io.SequenceFileVectorWriter;
import org.apache.mahout.utils.vectors.io.VectorWriter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;


/**
 * @version 1.0
 * @ClassName: VectorPrepare
 * @CreateTime: 2019-06-13 17:08
 */
public class VectorPrepare {
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.out.println("java -jar a.jar in out");
            return;
        }
        String infile = args[0];
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        Path outfile = new Path(args[1]);
        VectorWriter vectorWriter = new SequenceFileVectorWriter(SequenceFile.createWriter(fileSystem,
                conf,
                outfile,
                LongWritable.class,
                VectorWritable.class));
        ArrayList<Vector>  buf = new ArrayList<>();
        BufferedReader bufferedReader = new BufferedReader(new FileReader(infile));
        String line = "";
        while ((line = bufferedReader.readLine()) != null) {
            String[] cols = line.split(" ");
            if (cols.length != 3) {
                return;
            }
            double[] arr = new double[3];
            arr[0] = Double.parseDouble(cols[0]);
            arr[1] = Double.parseDouble(cols[1]);
            arr[2] = Double.parseDouble(cols[2]);
            Vector v = new DenseVector(arr);
            buf.add(v);
        }
        long numDocs = vectorWriter.write(buf, Long.MAX_VALUE);
        System.out.println("writing " + buf.size() + ", actually " + numDocs);
    }
}
