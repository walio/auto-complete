package NgramWithSmoothing.Calculation;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Lambda {
    public static class LambdaMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private Text predecessor = new Text();
        private DoubleWritable number = new DoubleWritable();
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
            number.set(Double.parseDouble(words[words.length-1]));
            context.write(predecessor,number);
        }
    }

    public static class LambdaReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {

        private double discount;
        @Override
        public void setup(Context context){
            discount = context.getConfiguration().getDouble("discount",0.75);
        }

        private DoubleWritable ValueOut = new DoubleWritable();
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum=0;
            int count=0;
            for(DoubleWritable value:values){
                sum+=value.get();
                ++count;
            }
            ValueOut.set(discount*count/sum);
            context.write(key,ValueOut);
        }
    }
}
