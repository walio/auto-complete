package NgramWithSmoothing.Calculator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Interpolator {
    public static class HigherProbMapper extends Mapper<LongWritable, Text, Text, Text> {

        private int higherOrder;
        private int lowerOrder;
        @Override
        public void setup(Context context){
            lowerOrder = context.getConfiguration().getInt("lowerOrder",0);
            higherOrder = context.getConfiguration().getInt("higherOrder",0);
        }

        private Text KeyOut = new Text();
        private Text ValueOut = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] words = line.split("\\s+");

            StringBuilder token = new StringBuilder(words[0]);
            for (int i=1;i<higherOrder-lowerOrder;++i){
                token.append(" ");
                token.append(words[i]);
            }

            StringBuilder lowerToken = new StringBuilder();
            for (int i=higherOrder-lowerOrder;i<higherOrder-1;++i){
                lowerToken.append(" ");
                lowerToken.append(words[i]);
            }

            KeyOut.set(lowerToken.toString().trim());
            ValueOut.set("higher:"+token+" "+words[higherOrder-1]+":"+words[higherOrder]);
            context.write(KeyOut,ValueOut);
        }
    }

    public static class LambdaMapper extends Mapper<LongWritable, Text, Text, Text> {

        private int higherOrder;
        private int lowerOrder;
        @Override
        public void setup(Context context){
            lowerOrder = context.getConfiguration().getInt("lowerOrder",0);
            higherOrder = context.getConfiguration().getInt("higherOrder",0);
        }

        private Text KeyOut = new Text();
        private Text ValueOut = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] words = line.split("\\s+");

            StringBuilder token = new StringBuilder(words[0]);
            for (int i=1;i<higherOrder-lowerOrder;++i){
                token.append(" ");
                token.append(words[i]);
            }

            StringBuilder lowerToken = new StringBuilder();
            for (int i=higherOrder-lowerOrder;i<higherOrder-1;++i){
                lowerToken.append(" ");
                lowerToken.append(words[i]);
            }

            KeyOut.set(lowerToken.toString().trim());
            ValueOut.set("lambda:"+token+":"+words[higherOrder-1]);
            context.write(KeyOut,ValueOut);
        }
    }

    public static class LowerProbMapper extends Mapper<LongWritable, Text, Text, Text> {

        private int lowerOrder;
        @Override
        public void setup(Context context){
            lowerOrder = context.getConfiguration().getInt("lowerOrder",0);
        }

        private Text KeyOut = new Text();
        private Text ValueOut = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] words = line.split("\\s+");

            StringBuilder lowerToken = new StringBuilder();
            for (int i=0;i<lowerOrder-1;++i){
                lowerToken.append(" ");
                lowerToken.append(words[i]);
            }

            KeyOut.set(lowerToken.toString().trim());
            ValueOut.set("lower:"+words[lowerOrder-1]+":"+words[lowerOrder]);
            context.write(KeyOut,ValueOut);
        }
    }

    public static class SummationReducer extends Reducer<Text,Text,Text,DoubleWritable> {

        private Text KeyOut = new Text();
        private DoubleWritable ValueOut = new DoubleWritable();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String,Double> lambda = new HashMap();
            Map<String,Double> lowerProb = new HashMap();
            Map<String,Double> higherProb = new HashMap();
            for (Text value:values){
                String[] words = value.toString().split(":");
                if (words[0].equals("lambda")){
                    lambda.put(words[1],Double.parseDouble(words[2]));
                }else if(words[0].equals("lower")){
                    lowerProb.put(words[1],Double.parseDouble(words[2]));
                }else{
                    higherProb.put(words[1],Double.parseDouble(words[2]));
                }
            }
            //todo:regexp
            for (Map.Entry<String, Double> higherEntry:higherProb.entrySet()){
                int splitter = higherEntry.getKey().lastIndexOf(" ");
                String pre = higherEntry.getKey().substring(0,splitter);
                String predict = higherEntry.getKey().substring(splitter+1);
                if (lambda.containsKey(pre) && lowerProb.containsKey(predict)){
                    KeyOut.set(pre+" "+key.toString()+" "+predict);
                    ValueOut.set(higherEntry.getValue()+lambda.get(pre)*lowerProb.get(predict));
                    context.write(KeyOut,ValueOut);
                }
            }
        }
    }

    public static void interpolate(String lowerProbPath,String higherProbPath,String lambdaPath,String outputPath,int higherOrder,int lowerOrder) throws ClassNotFoundException, IOException, InterruptedException {

        Configuration conf = new Configuration();
        conf.setInt("higherOrder",higherOrder);
        conf.setInt("lowerOrder",lowerOrder);

        Job job = Job.getInstance(conf);
        job.setJarByClass(Interpolator.class);

        job.setReducerClass(SummationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(higherProbPath), TextInputFormat.class, HigherProbMapper.class);
        MultipleInputs.addInputPath(job, new Path(lambdaPath), TextInputFormat.class, LambdaMapper.class);
        MultipleInputs.addInputPath(job, new Path(lowerProbPath), TextInputFormat.class, LowerProbMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.waitForCompletion(true);
    }
    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        interpolate(args[0],args[1],args[2],args[3],Integer.parseInt(args[4]),Integer.parseInt(args[5]));
    }
}
