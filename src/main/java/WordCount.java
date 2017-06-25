import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCount {

    public static class WordCount1Mapper extends Mapper<LongWritable,Text,Text,LongWritable>{

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            System.out.println(value);

            super.map(key, value, context);
        }
    }

    public static class WordCount1Reduce extends Reducer<Text,LongWritable,Text,LongWritable>{

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {

        WordCount wordCount1 = new WordCount();

        wordCount1.run(args);

    }

    public void run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1.configuration
        Configuration  conf = new Configuration();

        // 2.job
        Job job = Job.getInstance(conf,this.getClass().getSimpleName());
        // 3.jar
        job.setJarByClass(WordCount.class);

        // 4.input
        FileInputFormat.addInputPath(job,new Path(args[0]));

        // 5.mapper
        job.setMapperClass(WordCount1Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 6.reduce
        job.setReducerClass(WordCount1Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 7.out
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 8 start
        job.waitForCompletion(true);

    }
}
