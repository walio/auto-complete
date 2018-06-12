package NgramWithSmoothing.Calculation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Summation {

    public static class LowerProbMapper extends Mapper<LongWritable, Text, Text, Text>{


        private Text KeyOut = new Text();
        private Text ValueOut = new Text();
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
            ValueOut.set("lowerOrderProb:"+words[words.length-1]);

            context.write(KeyOut,ValueOut);
        }
    }

    public static class HigherProbMapper extends Mapper<LongWritable, Text, Text, Text>{


        private Text KeyOut = new Text();
        private Text ValueOut = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] words = line.split("\\s+");

            //todo: regexp
            StringBuilder lowerOrderGram = new StringBuilder(words[1]);
            for (int i=2;i<words.length-1;++i){
                lowerOrderGram.append(" ");
                lowerOrderGram.append(words[i]);
            }

            KeyOut.set(lowerOrderGram.toString());
            ValueOut.set("higherProb:"+words[0]+":"+words[words.length-1]);

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
            StringBuilder lowerOrderGram = new StringBuilder(words[1]);
            for (int i=2;i<words.length-1;++i){
                lowerOrderGram.append(" ");
                lowerOrderGram.append(words[i]);
            }

            KeyOut.set(lowerOrderGram.toString());
            ValueOut.set("lambda:"+words[0]+":"+words[words.length-1]);

            context.write(KeyOut,ValueOut);

        }
    }

    public static class SummationReducer extends Reducer<Text,Text,Text,DoubleWritable> {

        private Text KeyOut = new Text();
        private DoubleWritable ValueOut = new DoubleWritable();
        @Override
        //key: lower gram
        //valu: lambda:word:0.56/higherProb:word:0.68/lowerProb:0.56
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String,Double> lambda = new HashMap<>();
            Map<String,Double> higherProb = new HashMap<>();
            double lowerProb=0;
            for (Text value:values){
                String[] triple = value.toString().split(":");
                if(triple[0].equals("lambda")){
                    lambda.put(triple[1],Double.parseDouble(triple[2]));
                }else if (triple[0].equals("higherProb")){
                    higherProb.put(triple[1],Double.parseDouble(triple[2]));
                }else{
                    lowerProb=Double.parseDouble(triple[1]);
                }
            }
            for (Map.Entry<String, Double> entry : higherProb.entrySet()) {
                KeyOut.set(entry.getKey()+" "+key.toString());
                ValueOut.set(entry.getValue()+lambda.get(entry.getKey())*lowerProb);
                context.write(KeyOut,ValueOut);
            }
        }
    }

    public static void calculate(String inputPath,String outputPath,String lowerOrderInputPath,double discount) throws Exception{

        //calculate lambda,λ(w_(history))
        Configuration conf = new Configuration();
        conf.setDouble("discount",discount);

        Job LambdaJob = Job.getInstance(conf);
        LambdaJob.setJarByClass(Summation.class);

        LambdaJob.setMapperClass(Lambda.LambdaMapper.class);
        LambdaJob.setReducerClass(Lambda.LambdaReducer.class);

        LambdaJob.setOutputKeyClass(Text.class);
        LambdaJob.setOutputValueClass(Text.class);

        LambdaJob.setInputFormatClass(TextInputFormat.class);
        LambdaJob.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(LambdaJob, new Path(inputPath));
        TextOutputFormat.setOutputPath(LambdaJob, new Path(outputPath+"Lambda"));

        LambdaJob.waitForCompletion(true);

        //calculate higher Pkn
        Job ProbJob = Job.getInstance(conf);
        ProbJob.setJarByClass(Summation.class);

        ProbJob.setMapperClass(Probability.ProbMapper.class);
        ProbJob.setReducerClass(Probability.ProbReducer.class);

        ProbJob.setOutputKeyClass(Text.class);
        ProbJob.setOutputValueClass(Text.class);

        ProbJob.setInputFormatClass(TextInputFormat.class);
        ProbJob.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(ProbJob, new Path(inputPath));
        TextOutputFormat.setOutputPath(ProbJob, new Path(outputPath+"HigherProb"));

        ProbJob.waitForCompletion(true);

        //summation,(C{w(i−1)wi}−d)/C(wi−1)+λ(w_(history))*P(lowerOrder)
        Job SummationJob = Job.getInstance(conf);
        SummationJob.setJarByClass(Summation.class);

        SummationJob.setReducerClass(Summation.SummationReducer.class);

        SummationJob.setOutputKeyClass(Text.class);
        SummationJob.setOutputValueClass(DoubleWritable.class);

        SummationJob.setMapOutputValueClass(Text.class);

        MultipleInputs.addInputPath(SummationJob, new Path(outputPath+"Lambda"), TextInputFormat.class, Summation.LambdaMapper.class);
        MultipleInputs.addInputPath(SummationJob, new Path(outputPath+"HigherProb"), TextInputFormat.class, Summation.HigherProbMapper.class);
        MultipleInputs.addInputPath(SummationJob, new Path(lowerOrderInputPath), TextInputFormat.class, Summation.LowerProbMapper.class);

        TextOutputFormat.setOutputPath(SummationJob, new Path(outputPath));

        SummationJob.waitForCompletion(true);
    }
}