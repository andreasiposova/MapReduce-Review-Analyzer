package ProcessInput;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

    //This class counts the reviews per category, i.e.
    //for every review in a category, the output is key:category, value:1


public class RevsPerCat_CounterMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {


        public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
            JSONParser parser = new JSONParser();
            Object obj = null;
            try {
                obj = parser.parse(value.toString());
            } catch (ParseException e) {
                e.printStackTrace();
            }
            JSONObject jsonObject = (JSONObject) obj;
            String category = (String) jsonObject.get("category");

            context.write(new Text(category),new IntWritable(1));
        }
    }
