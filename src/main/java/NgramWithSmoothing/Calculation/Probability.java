package NgramWithSmoothing.Calculation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Probability {

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


    public static class ProbReducer extends Reducer<Text,Text,Text,Text> {


        private double discount;
        @Override
        public void setup(Context context){
            discount = context.getConfiguration().getDouble("discount",0.75);
        }

        private Text KeyOut = new Text();
        private Text ValueOut = new Text();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> grams=new ArrayList<>();
            List<Integer> counts=new ArrayList<>();
            int sum=0;
            for(Text value:values){
                grams.add(key.toString()+" "+value.toString().split("=")[0]);
                int count = Integer.parseInt(value.toString().split("=")[1]);
                sum+=count;
                counts.add(count);
            }
            for(int i=0;i<grams.size();++i){
                KeyOut.set(grams.get(i));
                ValueOut.set(String.valueOf((counts.get(i)-discount)/sum));
                context.write(KeyOut,ValueOut);
            }
        }
    }

    public static void calcProb(String inputPath,String outputPath,double discount) throws Exception{
        //calculate higher Pkn
        Configuration conf = new Configuration();
        conf.setDouble("discount",discount);

        Job ProbJob = Job.getInstance(conf);
        ProbJob.setJarByClass(Probability.class);

        ProbJob.setMapperClass(Probability.ProbMapper.class);
        ProbJob.setReducerClass(Probability.ProbReducer.class);

        ProbJob.setOutputKeyClass(Text.class);
        ProbJob.setOutputValueClass(Text.class);

        ProbJob.setInputFormatClass(TextInputFormat.class);
        ProbJob.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(ProbJob, new Path(inputPath));
        TextOutputFormat.setOutputPath(ProbJob, new Path(outputPath));

        ProbJob.waitForCompletion(true);
    }
}
