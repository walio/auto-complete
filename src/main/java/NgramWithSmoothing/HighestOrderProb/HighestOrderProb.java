package NgramWithSmoothing.HighestOrderProb;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HighestOrderProb {

    public static class HighestOrderProbMapper extends Mapper<LongWritable, Text, Text, Text>{

        int Ng;
        @Override
        public void setup(Context context){
            Ng = context.getConfiguration().getInt("Ng",5);
        }

        private Text KeyOut=new Text();
        private Text ValueOut=new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString().trim();
            String[] words = line.split("\\s+");
            if(words.length!=Ng+1){
                return;
            }

            StringBuilder sb = new StringBuilder(words[0]);
            for (int i=1;i<words.length-2;++i){
                sb.append(" ");
                sb.append(words[i]);
            }
            KeyOut.set(sb.toString());
            ValueOut.set(words[words.length-2]+"="+words[words.length-1]);
            context.write(KeyOut,ValueOut);
        }
    }

    public static class HighestOrderProbReducer extends Reducer<Text,Text,Text,DoubleWritable> {

        private double d;
        @Override
        public void setup(Context context){
            d=context.getConfiguration().getDouble("d",0.75);
        }

        private Text KeyOut = new Text();
        private DoubleWritable ValueOut = new DoubleWritable();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> grams=new ArrayList<>();
            List<Integer> counts=new ArrayList<>();
            int sum=0;
            for(Text value:values){
                grams.add(key.toString()+" "+value.toString().split("=")[0]);
                counts.add(Integer.parseInt(value.toString().split("=")[1]));
                sum+=Integer.parseInt(value.toString().split("=")[1]);
            }
            for(int i=0;i<counts.size();++i){
                KeyOut.set(grams.get(i));
                ValueOut.set((double)(counts.get(i)-d)/sum);
                context.write(KeyOut,ValueOut);
            }
        }
    }
}