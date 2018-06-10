package NgramWithSmoothing;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Summation {

    public static class TermMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{


        private Text KeyOut = new Text();
        private DoubleWritable ValueOut = new DoubleWritable();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString().trim();
            String[] words = line.split("\\s+");

            StringBuilder sb = new StringBuilder(words[0]);
            for (int i=1;i<words.length-2;++i){
                sb.append(" ");
                sb.append(words[i]);
            }

            KeyOut.set(sb.toString());
            ValueOut.set(Double.parseDouble(words[words.length-1]));
            context.write(KeyOut,ValueOut);

        }
    }

    public static class NgramReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {

        private DoubleWritable ValueOut = new DoubleWritable();
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            double sum=0;
            for(DoubleWritable value:values){
                sum+=value.get();
            }
            ValueOut.set(sum);
            context.write(key,ValueOut);
        }
    }
}