package TermFiltering;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

    // This reducer aggregates the keys (i.e. unique reviews) per combination of term;category
    // i.e. in how many reviews in a certain category a certain term is used

public class TermFilterReducer
        extends Reducer<Text, Text, Text, IntWritable> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<String> unique_revs = new HashSet<>();
        for (Text val : values) {
            unique_revs.add(val.toString());
        }
        context.write(key, new IntWritable(unique_revs.size()));
    }

}