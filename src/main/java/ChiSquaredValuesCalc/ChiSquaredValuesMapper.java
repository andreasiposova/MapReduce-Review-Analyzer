package ChiSquaredValuesCalc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.HashMap;

    // this class takes input from the previous mapper that prepared the needed values per term, and additionally takes input of the number of reviews per category from ProcessInput package
    //then calculates the Chi Squared

public class ChiSquaredValuesMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        long total_revs = 1;
        String[] split_st = value.toString().split("\\s+");
        String term = split_st[0];
        String[] catAndCounts = split_st[1].split(",");
        HashMap<String, Integer> category_count = new HashMap<>();
        HashMap<String, Integer> allCats = new HashMap<>();

        Configuration config = context.getConfiguration();
        
        String revsPerCat = config.get("reviews_per_category");
        String[] revsPerCatsplit = revsPerCat.split(",");
        for (String cat : revsPerCatsplit){
            String [] cat_Count = cat.split("\\s+");
            allCats.put(cat_Count[0], Integer.parseInt(cat_Count[1]));
        }

        for (String cat:catAndCounts) {
            category_count.put(cat.split(";")[0], Integer.parseInt(cat.split(";")[1]));
        }
        
        // First the A,B,C,D values are calculated according to the formula and then the Chi Squared values
        // Using BigDecimal for the interim results and Chi Square, it requires precision (20 used to min the difference)

        double A = 0;
        double B = 0;
        double C=0;
        double D=0;
        BigDecimal chisq= BigDecimal.valueOf(0);
        MathContext mc = new MathContext(20);

        for (String cat:allCats.keySet()) {
            if (category_count.containsKey(cat)) {
                A = category_count.get(cat);
            }
            for (String catB:category_count.keySet()) {
                if(!cat.equals(catB)){
                    B +=category_count.get(catB);
                }
            }
            C = allCats.get(cat)-A;

            for (String catD:allCats.keySet()) {
                if(!cat.equals(catD)) {
                    D=allCats.get(catD);
                    if (category_count.containsKey(catD)) {
                        D = D-category_count.get(catD);
                    }
                }
            }
            //calculating interim results below
            //numerator of the chi square formula
            BigDecimal AD = BigDecimal.valueOf(A*D);
            BigDecimal BC = BigDecimal.valueOf(B*C);
            BigDecimal numerator = AD.subtract(BC);
            // I set the number of total reviews to 1
            // as we multiply the denominator with it, and as it does not change the order of the chi square values
            BigDecimal bd_revs_total = BigDecimal.valueOf(total_revs);
            numerator = bd_revs_total.multiply(numerator.pow(2));

            //denominator of the chi square formula
            BigDecimal AB = BigDecimal.valueOf(A+B);
            BigDecimal AC = BigDecimal.valueOf(A+C);
            BigDecimal BD = BigDecimal.valueOf(B+D);
            BigDecimal CD = BigDecimal.valueOf(C+D);

            BigDecimal part1 = AB.multiply(AC);
            BigDecimal part2 = part1.multiply(BD);
            BigDecimal denominator = part2.multiply(CD);

            //put it together to calculate chi square
            chisq =  numerator.divide(denominator,mc);
            //write the value
            context.write(new Text(term+";"+cat), new Text(String.valueOf(chisq)));
            A=0; B=0; C=0; D=0; chisq= BigDecimal.valueOf(0);
        }


    }


}

