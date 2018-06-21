package NgramWithSmoothing.KgramExtractor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class CKNExtractor {
    public static class ExtractMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        int Gramlength;
        @Override
        public void setup(Context context){
            Gramlength = context.getConfiguration().getInt("GramLength",4);
        }

        private Text word=new Text();
        private IntWritable one = new IntWritable(1);
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString().trim();
            String[] words = line.split("\\s+");

            if(words.length!=Gramlength+2){
                return;
            }

            //todo:regexp
            StringBuilder sb = new StringBuilder(words[1]);
            for (int i=2;i<Gramlength+1;++i){
                sb.append(" ");
                sb.append(words[i]);
            }
            word.set(sb.toString());
            context.write(word, one);

        }
    }

    public static class ExtractReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable ValueOut = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable i:values){
                ++sum;
            }
            ValueOut.set(sum);
            context.write(key,ValueOut);
        }
    }

    public static void extract(String inputPath,String outputPath,int order) throws ClassNotFoundException, IOException, InterruptedException {
        //extract cardinality of k gram
        Configuration conf = new Configuration();
        conf.setInt("GramLength",order);

        Job job = Job.getInstance(conf);
        job.setJarByClass(CKNExtractor.class);

        job.setMapperClass(ExtractMapper.class);
        job.setReducerClass(ExtractReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job, new Path(inputPath));
        TextOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);

    }
    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        extract(args[0],args[1],Integer.parseInt(args[2]));
    }
}
