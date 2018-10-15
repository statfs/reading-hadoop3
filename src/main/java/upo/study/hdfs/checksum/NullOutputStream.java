package upo.study.hdfs.checksum;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by aanand on 07/11/16.
 */
public class NullOutputStream extends OutputStream {
    @Override
    public void write(int b) throws IOException {
    }
}
