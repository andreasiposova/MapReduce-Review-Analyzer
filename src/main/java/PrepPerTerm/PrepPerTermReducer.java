package PrepPerTerm;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

    //this reducer aggregates the data per term and category;1
    // to output the number of reviews per category per each term
    // example:  key:term, value: category1;4 ,category2;7...
    // a certain term has been used in 4 reviews in one category, in 7 reviews in another category.


public class PrepPerTermReducer
        extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
        String output = "";
        for (Text val : values) {
            output = output + val.toString()+ "," ;
        }
        context.write(key, new Text(output.substring(0,output.length()-1)));

    }
}