import AlphabeticalSort.AlphabeticalSortMapper;
import AlphabeticalSort.AlphabeticalSortReducer;
import ChiSquaredValuesCalc.ChiSquaredValuesMapper;
import PrepPerTerm.PrepPerTermMapper;
import PrepPerTerm.PrepPerTermReducer;
import ProcessInput.RevsPerCat_CounterMapper;
import ProcessInput.RevsPerCat_CounterReducer;
import TermFiltering.TermFilterMapper;
import TermFiltering.TermFilterReducer;
import Top150terms.Top150termsMapper;
import Top150terms.Top150termsReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class Main {

    public static final String inputFilePath = "/user/pknees/amazon-reviews/full/reviewscombined.json";
    public static final String outputDirectoryPath = "RevsPerCategory_Counter";
    public static final String outputDirectoryFilter = "TermsFiltered";
    public static final String chiSquaredValDirectory = "PreparedPerTerm";
    public static final String outputChiSq = "ChiSquaredValues";
    public static final String outputTop150terms = "Top150terms";
    public static final String outputTop150termsSorted = "Top150termsSorted";


    public static void main(String[] args) throws Exception {
          BasicConfigurator.configure();
          RevsPerCategory_Counter(inputFilePath, outputDirectoryPath);
          termFilter(inputFilePath,outputDirectoryFilter);
          prepareTerms_ChiSq(outputDirectoryFilter,chiSquaredValDirectory);
          chiSquaredValuesCalc(chiSquaredValDirectory,outputChiSq);
          top150Terms(outputChiSq,outputTop150terms);
          SortAlphabetOrder(outputTop150terms,outputTop150termsSorted);
    }

    private static void RevsPerCategory_Counter(String inputFilePath, String outputDirectoryPath) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setNumReduceTasks(1);

        job.setJarByClass(Main.class);
        job.setMapperClass(RevsPerCat_CounterMapper.class);
        job.setCombinerClass(RevsPerCat_CounterReducer.class);
        job.setReducerClass(RevsPerCat_CounterReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputFilePath));
        FileOutputFormat.setOutputPath(job, new Path(outputDirectoryPath, outputDirectoryPath));
        boolean status = job.waitForCompletion(true);
        if (!status) {
            System.exit(1);
        }
    }

    private static void termFilter(String inputFilePath, String outputDirectoryFilter) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "term_filter");
        job.setNumReduceTasks(8);

        job.setJarByClass(Main.class);
        job.setMapperClass(TermFilterMapper.class);
        job.setReducerClass(TermFilterReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class); //

        FileInputFormat.addInputPath(job, new Path(inputFilePath));
        FileOutputFormat.setOutputPath(job, new Path(outputDirectoryFilter));
        boolean status = job.waitForCompletion(true);
        if (!status) {
            System.exit(1);
        }
    }

    private static void prepareTerms_ChiSq(String outputDirectoryFilter, String chiSquaredValDirectory) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "per_term");
        job.setNumReduceTasks(8);

        job.setJarByClass(PrepPerTermMapper.class);
        job.setMapperClass(PrepPerTermMapper.class);
        job.setReducerClass(PrepPerTermReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class); //

        FileInputFormat.addInputPath(job, new Path(outputDirectoryFilter));
        FileOutputFormat.setOutputPath(job, new Path(chiSquaredValDirectory));
        boolean status = job.waitForCompletion(true);
        if (!status) {
            System.exit(1);
        }
    }

    private static void chiSquaredValuesCalc(String chiSquaredValDirectory, String outputChiSq) throws Exception {
        Configuration conf = new Configuration();

        // read reviews per category from RevsPerCategory_Counter
        String reviews_per_category = "";
        FileSystem fileSystem = FileSystem.get(conf);
        Path readPath = new Path("RevsPerCategory_Counter/RevsPerCategory_Counter/part-r-00000");
        FSDataInputStream inputStream = fileSystem.open(readPath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));

        String line = null;
        while ((line=bufferedReader.readLine())!=null){
            reviews_per_category += line + ",";
        }
        reviews_per_category = reviews_per_category.substring(0, reviews_per_category.length() -1);
        conf.set("reviews_per_category", reviews_per_category);

        Job job = Job.getInstance(conf, "calc_chisq");
        job.setNumReduceTasks(0);

        job.setJarByClass(Main.class);
        job.setMapperClass(ChiSquaredValuesMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class); //

        FileInputFormat.addInputPath(job, new Path(chiSquaredValDirectory));
        FileOutputFormat.setOutputPath(job, new Path(outputChiSq));
        boolean status = job.waitForCompletion(true);
        if (!status) {
            System.exit(1);
        }
    }

    private static void top150Terms(String outputChiSq, String outputTop150terms) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "gettop150terms");
        job.setNumReduceTasks(8);

        job.setJarByClass(Main.class);
        job.setMapperClass(Top150termsMapper.class);
        job.setReducerClass(Top150termsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class); //

        FileInputFormat.addInputPath(job, new Path(outputChiSq));
        FileOutputFormat.setOutputPath(job, new Path(outputTop150terms));
        boolean status = job.waitForCompletion(true);
        if (!status) {
            System.exit(1);
        }
    }

    private static void SortAlphabetOrder(String outputTop150terms, String outputTop150termsSorted) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "alphabetsort");
        job.setNumReduceTasks(1);

        job.setJarByClass(Main.class);
        job.setMapperClass(AlphabeticalSortMapper.class);
        job.setReducerClass(AlphabeticalSortReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class); //

        FileInputFormat.addInputPath(job, new Path(outputTop150terms));
        FileOutputFormat.setOutputPath(job, new Path(outputTop150termsSorted));
        boolean status = job.waitForCompletion(true);
        if (!status) {
            System.exit(1);
        }
    }
}
