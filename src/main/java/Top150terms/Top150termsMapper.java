package Top150terms;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
// This mapper takes the input from the ChiSquaredCalc package
// and outputs the data in the following format: key:category, value:term;chisq
//
public class Top150termsMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split_st = value.toString().split("\\s+");
        String[] termCat = split_st[0].split(";");
        context.write(new Text(termCat[1]), new Text(termCat[0]+";" + split_st[1]));
    }
}