package PrepPerTerm;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

    //This class takes the input from the term filter,
    // and the mapper outputs the category and 1 from the data per term, i.e. key:term value:category;1


public class PrepPerTermMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split_st = value.toString().split("\\s+");   
        String term_cat = split_st[0];
        String term = term_cat.split(";")[0];
        String category = term_cat.split(";")[1] + ";" + split_st[1];
        context.write(new Text(term), new Text(category));
    }
}

