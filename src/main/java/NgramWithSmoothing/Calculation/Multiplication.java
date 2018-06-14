package NgramWithSmoothing.Calculation;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Multiplication {

    public static class LowerProbMapper extends Mapper<LongWritable, Text, Text, Text>{


        private Text KeyOut = new Text();
        private Text ValueOut = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] words = line.split("\\s+");

            //todo: regexp
            StringBuilder lowerOrderGram = new StringBuilder();
            for (int i=0;i<words.length-2;++i){
                lowerOrderGram.append(" ");
                lowerOrderGram.append(words[i]);
            }

            KeyOut.set(lowerOrderGram.toString());
            ValueOut.set("lowerOrderProb:"+words[words.length-2]+":"+words[words.length-1]);

            context.write(KeyOut,ValueOut);
        }
    }

    public static class LambdaMapper extends Mapper<LongWritable, Text, Text, Text>{


        private Text KeyOut = new Text();
        private Text ValueOut = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString().trim();
            String[] words = line.split("\\s+");

            //todo: regexp
            StringBuilder lowerOrderGram = new StringBuilder();
            for (int i=1;i<words.length-1;++i){
                lowerOrderGram.append(" ");
                lowerOrderGram.append(words[i]);
            }

            KeyOut.set(lowerOrderGram.toString());
            ValueOut.set("lambda:"+words[0]+":"+words[words.length-1]);

            context.write(KeyOut,ValueOut);

        }
    }

    public static class MultiplicationReducer extends Reducer<Text,Text,Text,DoubleWritable> {

        private Text KeyOut = new Text();
        private DoubleWritable ValueOut = new DoubleWritable();
        @Override
        //make Cartesian Product
        //key: lower gram
        //valu:

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String,Double> lambda = new HashMap<>();
            Map<String,Double> lowerProb = new HashMap<>();
            for (Text value:values){
                String[] triple = value.toString().split(":");
                if(triple[0].equals("lambda")){
                    lambda.put(triple[1],Double.parseDouble(triple[2]));
                }else{
                    lowerProb.put(triple[1],Double.parseDouble(triple[2]));
                }
            }
            //todo
            for (Map.Entry<String, Double> lambdaEntry : lambda.entrySet()) {
                for (Map.Entry<String, Double> lowerProbEntry : lowerProb.entrySet()){
                    KeyOut.set(lambdaEntry.getKey()+" "+key.toString()+" "+lowerProbEntry.getKey());
                    ValueOut.set(lambdaEntry.getValue()*lowerProbEntry.getValue());
                    context.write(KeyOut,ValueOut);
                }
            }
        }
    }
}