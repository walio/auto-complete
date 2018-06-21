package NgramWithSmoothing.Calculator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ProbabilityCalculator {

    //the same as lambda.lambdamapper
    public static class ProbMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text predecessor = new Text();
        private Text counter = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString().trim();
            String[] words = line.split("\\s+");

            //todo: regexp
            StringBuilder sb = new StringBuilder();
            for (int i=0;i<words.length-2;++i){
                sb.append(" ");
                sb.append(words[i]);
            }
            predecessor.set(sb.toString().trim());
            counter.set(words[words.length-2]+"="+words[words.length-1]);
            context.write(predecessor,counter);
        }
    }


    public static class ProbReducer extends Reducer<Text,Text,Text,DoubleWritable> {


        private double discount;
        @Override
        public void setup(Context context){
            discount = context.getConfiguration().getDouble("discount",0.75);
        }

        private Text KeyOut = new Text();
        private DoubleWritable ValueOut = new DoubleWritable();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> grams=new ArrayList<>();
            List<Integer> counts=new ArrayList<>();
            int sum=0;
            for(Text value:values){
                grams.add(value.toString().split("=")[0]);
                int count = Integer.parseInt(value.toString().split("=")[1]);
                sum+=count;
                counts.add(count);
            }
            for(int i=0;i<grams.size();++i){
                KeyOut.set(key.toString()+" "+grams.get(i));
                ValueOut.set((counts.get(i)-discount)/sum);
                context.write(KeyOut,ValueOut);
            }
        }
    }



    public static void calcProb(String inputPath,String outputPath,double discount) throws ClassNotFoundException, IOException, InterruptedException {
        //calculate higher Pkn
        Configuration conf = new Configuration();
        conf.setDouble("discount",discount);

        Job job = Job.getInstance(conf);
        job.setJarByClass(ProbabilityCalculator.class);

        job.setMapperClass(ProbMapper.class);
        job.setReducerClass(ProbReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job, new Path(inputPath));
        TextOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }
    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        calcProb(args[0],args[1],Double.parseDouble(args[2]));
    }
}
