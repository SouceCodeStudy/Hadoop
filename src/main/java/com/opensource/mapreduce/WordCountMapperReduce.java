package com.opensource.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * 统计文件中的单词数量
 *
 */
public class WordCountMapperReduce {

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {

        WordCountMapperReduce wordCount = new WordCountMapperReduce();

        String[] arguments = {
                "/user/hadoop/mapreduce/wordcount/input",
                "/user/hadoop/mapreduce/wordcount/output"
        };

        Configuration  conf = new Configuration();
        // 删除输出的目录
        wordCount.deleteOutputPath(arguments[1],conf);
        // 运行mapreduce
        wordCount.run(arguments,conf);
        // 读取统计的结果如下格式
        /**
         * hadoop	4
         * hdfs	2
         * mapreduce	2
         * nodemanager	2
         * yarn	3
         */
        wordCount.readOutputContent(arguments[1], conf);
    }

    /**
     * 删除输出的目录包括下面的全部文件
     * @param directoryPath
     * @param conf
     * @throws IOException
     */
    public void deleteOutputPath(String directoryPath,Configuration  conf) throws IOException {

        FileSystem file = FileSystem.get(conf);
        //  当输出的目录存在的场合，删除当前目录
        if(file.isDirectory(new Path(directoryPath))) {

            file.delete(new Path(directoryPath),true);

        }
    }

    public void readOutputContent(String directoryPath,Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream dataInputStream = null;
        try {
            dataInputStream = fs.open(new Path(directoryPath + File.separator + "part-r-00000"));
            IOUtils.copyBytes(dataInputStream,System.out,4096,true);
        } catch(Exception e) {

        } finally {
            if(null != dataInputStream) {
                IOUtils.closeStream(dataInputStream);
            }
        }

    }
    public static class WordCountReduce2 extends Reducer<Text,LongWritable,Text,LongWritable> {


        public  void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            Long totalCount = Long.valueOf(1);

            for(LongWritable value:values) {
                totalCount += value.get();
            }
            context.write(key,new LongWritable(totalCount));
        }

    }
    /**
     * 运行MapReduce程序
     * 如果用命令打包执行在yarn上
     *         >> bin/yarn jar xxxx.jar inputPath outputPath
     * @param args
     * @param configuration
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void run(String[] args,Configuration configuration) throws IOException, ClassNotFoundException, InterruptedException {

        // 1.configuration
        Configuration  conf = configuration;

        // 2.job
        Job job = Job.getInstance(conf,this.getClass().getSimpleName());
        // 3.jar
        job.setJarByClass(WordCountMapperReduce.class);

        // 4.input
        FileInputFormat.addInputPath(job,new Path(args[0]));

        // 5.mapper
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 6.reduce
        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 7.out
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 8 start
        // 参数 true r打印日志信息
        job.waitForCompletion(true);

    }
}
