package ProcessInput;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//This class takes the output of the mapper and aggregates it
// so the reducer output is key:category, value:number of reviews per category

public class RevsPerCat_CounterReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException{
        int nc = 0;
        for (IntWritable val : values) {
            nc += val.get();
        }
        context.write(key, new IntWritable(nc));
    }
}
