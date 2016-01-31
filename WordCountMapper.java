import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
        private static final IntWritable STANDARD_VALUE = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
                String values = value.toString();
                values = values.replaceAll("[!?,'\"]","");
                List<String> wordList = Arrays.asList( values.split("\\s+") );

                for(String word : wordList)
                {
                        context.write( new Text( word ), STANDARD_VALUE );
                }
        }
}
