package NgramWithSmoothing.Calculation;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Summation {
    public static class SummationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

        private Text KeyOut = new Text();
        private DoubleWritable ValueOut = new DoubleWritable();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] words = line.split("\\s+");

            //todo: regexp
            StringBuilder lowerOrderGram = new StringBuilder(words[0]);
            for (int i=1;i<words.length-1;++i){
                lowerOrderGram.append(" ");
                lowerOrderGram.append(words[i]);
            }

            KeyOut.set(lowerOrderGram.toString());
            ValueOut.set(Double.parseDouble(words[words.length-1]));

            context.write(KeyOut,ValueOut);

        }
    }

    public static class SummationReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {

        private DoubleWritable ValueOut = new DoubleWritable();
        @Override
        //add higher prob and interpolation
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum=0;
            for (DoubleWritable value:values){
                sum+=value.get();
            }
            ValueOut.set(sum);
            context.write(key,ValueOut);
        }
    }
}