package NgramWithSmoothing.Calculator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class LambdaCalculator {
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

    public static class LambdaPartitioner extends Partitioner<Text,Text> {

        @Override
        public int getPartition(Text key, Text value,int numPartitions){
            return (int)key.toString().charAt(0);
        }
    }

    public static void calcLambda(String inputPath,String outputPath,double discount) throws ClassNotFoundException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setDouble("discount",discount);

        Job job = Job.getInstance(conf);
        job.setJarByClass(LambdaCalculator.class);

        job.setMapperClass(LambdaMapper.class);
        job.setPartitionerClass(LambdaPartitioner.class);
        job.setReducerClass(LambdaReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job, new Path(inputPath));
        TextOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }
    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        calcLambda(args[0],args[1],Double.parseDouble(args[2]));
    }
}
