import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.IOException;

/**
 * XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
 *
 * @author YX_user
 * @version 1.0.0 2017/6/24
 * @name PACKAGE_NAME
 */
public class HDFS {

    public static void main(String[] args) throws IOException {

        String fileName = "/user/hadoop/mapreduce/wordcount/input/wc.input";

        readHDFSFile(fileName);


    }

    public static void readHDFSFile(String fileName) throws IOException {

        FileSystem fs = getFileSystem();

        Path fileNamePath = new Path(fileName);

        FSDataInputStream inputStream = fs.open(fileNamePath);

        try{
            IOUtils.copyBytes(inputStream,System.out,4096,false);
        }catch (Exception e){
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(inputStream);
        }
    }

    public static FileSystem getFileSystem() throws IOException {
        // core-site.xml,core-default.xml
        // hdfs-site.xml,hdfs-default.xml
        Configuration conf = new Configuration();
        // get filesystem
        FileSystem fs = FileSystem.get(conf);

        return fs;
    }

}
