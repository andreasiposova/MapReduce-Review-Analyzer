package AlphabeticalSort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

    //The mapper takes as input the output from the top150terms package
    //outputs a key for each category and the value is category_term1:chisq,term2:chisq
    //so the categories can be ordered alphabetically, and so the terms from that category stay in the same line with the category

public class AlphabeticalSortMapper
        extends Mapper<LongWritable, Text, IntWritable, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split_st = value.toString().split("\\s+");
        String output = split_st[0] + ";" + split_st[1];
        context.write(new IntWritable(1), new Text(output));
    }
}
