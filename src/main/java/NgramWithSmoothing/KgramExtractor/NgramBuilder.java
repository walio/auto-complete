package NgramWithSmoothing.KgramExtractor;

import NgramWithSmoothing.Driver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class NgramBuilder {

    public static class NgramMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

        int Ng;
        @Override
        public void setup(Context context){
            Ng = context.getConfiguration().getInt("Ng",5);
        }

        private IntWritable one = new IntWritable(1);
        private Text word=new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            line = line.trim().toLowerCase();
            line = line.replaceAll("[^a-z]", " ");
            String[] words = line.split("\\s+");

            for(int i=0;i<words.length-1;++i){
                StringBuilder sb = new StringBuilder();
                for(int corlen=0;corlen<Ng && corlen+i<words.length;++corlen){
                    sb.append(" ");
                    sb.append(words[corlen+i]);
                    word.set(sb.toString().trim());
                    context.write(word,one);
                    context.progress();
                }
            }
        }
    }

    public static class NgramReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        private int threshold;
        @Override
        public void setup(Context context){
            threshold=context.getConfiguration().getInt("threshold",5);
        }

        private IntWritable count = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
                context.progress();
            }
            if(sum<threshold){
                return;
            }
            count.set(sum);
            context.write(key, count);
        }
    }

    public static void buildNgram(String inputPath,String outputPath,int Ng,int threshold) throws ClassNotFoundException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("textinputformat.record.delimiter", ".");
        conf.setInt("Ng",Ng);
        conf.setInt("threshold",threshold);

        Job job = Job.getInstance(conf);
        job.setJarByClass(Driver.class);

        job.setMapperClass(NgramMapper.class);
        job.setReducerClass(NgramReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job, new Path(inputPath));
        TextOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception{
        buildNgram(args[0],args[1],Integer.parseInt(args[2]),Integer.parseInt(args[3]));
    }

}