import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 单词统计的MAPPER
 */
public   class  WordCountMapper extends Mapper<LongWritable,Text,Text,LongWritable>{

    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        StringTokenizer tokens = new StringTokenizer(value.toString());

        while(tokens.hasMoreTokens()) {
            //  取得下一个数据
            String text = tokens.nextToken();
            context.write(new Text(text),new LongWritable(1));
        }


    }
}
