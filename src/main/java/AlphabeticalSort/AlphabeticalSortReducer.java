package AlphabeticalSort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

    // This reducer takes as input the output of the mapper from the same package, splits the values on the chosen separator (;) between the category and its terms with the respective chisq values
    // outputs key:category, value:sorted terms with their resp. chisq values, where the lines are sorted alphabetically according to category name


public class AlphabeticalSortReducer
        extends Reducer<IntWritable, Text, Text, Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String, String> catTerm = new HashMap<>();
        for (Text val:values){
            String[] valStr = val.toString().split(";");
            String cat = valStr[0];
            String terms = valStr[1];
            catTerm.put(cat, terms);
        }

        TreeMap<String, String> catTree = new TreeMap<>(catTerm);
        Iterator<String> itr = catTree.	keySet().iterator();
        while (itr.hasNext()) {
            String category = itr.next();
            String terms = catTerm.get(category);
            context.write(new Text(category), new Text(terms));
        }

    }
}