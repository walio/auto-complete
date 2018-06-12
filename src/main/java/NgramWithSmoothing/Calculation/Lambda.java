package NgramWithSmoothing.Calculation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Lambda {
    public static class LambdaMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text predecessor = new Text();
        private Text counter = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString().trim();
            String[] words = line.split("\\s+");

            //todo: regexp
            StringBuilder sb = new StringBuilder(words[0]);
            for (int i=1;i<words.length-2;++i){
                sb.append(" ");
                sb.append(words[i]);
            }
            predecessor.set(sb.toString());
            counter.set(words[words.length-2]+" "+words[words.length-1]);
            context.write(predecessor,counter);
        }
    }

    public static class LambdaReducer extends Reducer<Text,Text,Text,Text> {

        private double discount;
        @Override
        public void setup(Context context){
            discount = context.getConfiguration().getDouble("d",0.75);
        }

        private Text KeyOut = new Text();
        private Text ValueOut = new Text();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> grams=new ArrayList<>();
            int sum=0;
            int count=0;
            for(Text value:values){
                grams.add(key.toString()+" "+value.toString().split("\\s+")[0]);
                sum+=Integer.parseInt(value.toString().split("\\s+")[1]);
                ++count;
            }
            for(String gram:grams){
                KeyOut.set(gram);
                ValueOut.set(String.valueOf((double)discount*count/sum));
                context.write(KeyOut,ValueOut);
            }
        }
    }
}