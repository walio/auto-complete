package NgramWithSmoothing.Continuation;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Probability {
    public static class ProbMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text KeyOut = new Text();
        private Text ValueOut = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString().trim();
            String[] words = line.split("\\s+");

            StringBuilder sb = new StringBuilder(words[0]);
            for (int i=1;i<words.length-2;++i){
                sb.append(" ");
                sb.append(words[i]);
            }
            KeyOut.set(words[words.length-2]);
            ValueOut.set(sb.toString());
            context.write(KeyOut,ValueOut);
        }
    }

    public static class ProbReducer extends Reducer<Text,Text,Text,Text> {


        private long total;
        @Override
        //todo:defaultValue?
        public void setup(Context context){
            total = context.getConfiguration().getLong("total",1);
        }

        private Text KeyOut = new Text();
        private Text ValueOut = new Text();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> grams=new ArrayList<>();
            int count=0;
            for(Text value:values){
                grams.add(value.toString()+" "+key.toString());
                ++count;
            }
            for(String gram:grams){
                KeyOut.set(gram);
                ValueOut.set(String.valueOf((double)count/total));
                context.write(KeyOut,ValueOut);
            }
        }
    }
}
