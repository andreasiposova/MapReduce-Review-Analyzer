package TermFiltering;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

        //This classes preprocesses the input data, filters out stop-words, punctuation signs/delimiters and digits, and converts to lowercase
        // then gets the respective category for each term and review ID (to get that the "reviewerID" and "asin" as combined)

public class TermFilterMapper
        extends Mapper<LongWritable, Text, Text, Text> {


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
        String review = (String) jsonObject.get("reviewText");
        String doc_ID = (String) jsonObject.get("reviewerID") + (String) jsonObject.get("asin");

        //add in stop-words to be removed
        List<String> stopwords =  Arrays.asList("a","aa","able","about","above","according","accordingly","across","actually","after","afterwards","again","against","ain","all","allow","allows","almost","alone","along","already","also","although","always","am","among","amongst","an","and","another","any","anybody","anyhow","anyone","anything","anyway","anyways","anywhere","apart","appear","appreciate","appropriate","are","aren","around","as","aside","ask","asking","associated","at","available","away","awfully","b","bb","be","became","because","become","becomes","becoming","been","before","beforehand","behind","being","believe","below","beside","besides","best","better","between","beyond","bibs","book","both","brief","but","by","c","came","can","cannot","cant","car","cause","causes","cd","certain","certainly","changes","clearly","co","com","come","comes","concerning","consequently","consider","considering","contain","containing","contains","corresponding","could","couldn","course","currently","d","definitely","described","despite","did","didn","different","do","does","doesn","doing","don","done","down","downwards","during","e","each","edu","eg","eight","either","else","elsewhere","enough","entirely","especially","et","etc","even","ever","every","everybody","everyone","everything","everywhere","ex","exactly","example","except","f","far","few","fifth","first","five","followed","following","follows","for","former","formerly","forth","four","from","further","furthermore","g","game","game","get","gets","getting","given","gives","go","goes","going","gone","got","gotten","greetings","h","had","hadn","happens","hardly","has","hasn","have","haven","having","he","hello","help","hence","her","here","hereafter","hereby","herein","hereupon","hers","herself","hi","him","himself","his","hither","hopefully","how","howbeit","however","i","ie","if","ignored","immediate","in","inasmuch","inc","indeed","indicate","indicated","indicates","inner","insofar","instead","into","inward","is","isn","it","its","itself","j","just","k","keep","keeps","kept","know","known","knows","l","last","lately","later","latter","latterly","least","less","lest","let","life","like","liked","likely","little","ll","look","looking","looks","ltd","m","mainly","many","may","maybe","me","mean","meanwhile","merely","might","mon","more","moreover","most","mostly","much","must","my","myself","n","name","namely","nd","near","nearly","necessary","need","needs","neither","never","nevertheless","new","next","nine","no","nobody","non","none","noone","nor","normally","not","nothing","novel","now","nowhere","o","obviously","of","off","often","oh","ok","okay","old","on","once","one","ones","only","onto","or","other","others","otherwise","ought","our","ours","ourselves","out","outside","over","overall","own","p","particular","particularly","per","perhaps","placed","please","plus","possible","presumably","probably","provides","q","que","quite","qv","r","rather","rd","re","really","reasonably","regarding","regardless","regards","relatively","respectively","right","s","said","same","saw","say","saying","says","second","secondly","see","seeing","seem","seemed","seeming","seems","seen","self","selves","sensible","sent","serious","seriously","seven","several","shall","she","should","shouldn","since","six","so","some","somebody","somehow","someone","something","sometime","sometimes","somewhat","somewhere","soon","sorry","specified","specify","specifying","still","sub","such","sup","sure","t","take","taken","tell","tends","th","than","thank","thanks","thanx","that","that","thats","the","their","theirs","them","themselves","then","thence","there","there","thereafter","thereby","therefore","therein","theres","thereupon","these","they","think","third","this","thorough","thoroughly","those","though","three","through","throughout","thru","thus","to","together","too","took","toward","towards","tried","tries","truly","try","trying","twice","two","u","un","under","unfortunately","unless","unlikely","until","unto","up","upon","us","use","used","useful","uses","using","usually","v","value","various","ve","very","via","viz","vs","want","wants","was","wasn","way","we","welcome","well","went","were","weren","what","whatever","when","whence","whenever","where","whereafter","whereas","whereby","wherein","whereupon","wherever","whether","which","while","whither","who","whoever","whole","whom","whose","why","will","willing","wish","with","within","without","won","wonder","would","wouldn","x","y","yes","yet","you","your","yours","yourself","yourselves","z","zero");

        //remove punctuation, numbers and stop-words
        StringTokenizer delims = new StringTokenizer(review, " .!?,;:()[]{}-_\"'`~#&*%$\\/");
        while (delims.hasMoreTokens()) {
            String delim = delims.nextToken();
            for (String t:delim.split("[0-9]+")) {
                if (t.length()>1) {
                    String term = t.toLowerCase();
                    if (!stopwords.contains(term)){
                        context.write(new Text(term + ";" + category),new Text(doc_ID));
                    }

                }
            }
        }
    }
}