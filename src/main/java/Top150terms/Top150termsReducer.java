package Top150terms;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

//The reducer takes input from the previous mapper key:category, value:term;chisq
//aggregates the chi squared values to output "key:category, value: term1:chisq, term2:chisq, ..." in the required format
//i.e. the category and the terms used in that category with their respective chisq values

public class Top150termsReducer
        extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<Double, String> term_chisq = new HashMap<Double, String>();
        for (Text val:values){
            String[] valStr = val.toString().split(";");
            Double chisq = Double.parseDouble(valStr[1]);
            String term = valStr[0];
            term_chisq.put(chisq, term);
        }
        // Order the Chi Squared values
        // and iterate over decreasing Chi Squared values
        // choose top 150 terms
        TreeMap<Double, String> category_chisq_Tree = new TreeMap<Double, String>(term_chisq);
        String output = "";

        Iterator<Double> itr = category_chisq_Tree.descendingKeySet().iterator();
        for (int i=0;i<=150;i++) {
            Double key_chisquare = itr.next();
            String term = term_chisq.get(key_chisquare);
        output = output + term + ":"+ Double.toString(key_chisquare) + ",";
        }
        context.write(key, new Text(output.substring(0, output.length()-1)));
    }
}