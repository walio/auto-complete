package NgramWithSmoothing;

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
                }
            }
        }
    }

    public static class NgramReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        private int threshold;
        @Override
        public void setup(Context context){
            threshold=context.getConfiguration().getInt("threshold",10);
        }

        private IntWritable count = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            if(sum<threshold){
                return;
            }
            count.set(sum);
            context.write(key, count);
        }
    }

    public static void buildNgram(String inputPath,String outputPath) throws Exception{
        Configuration conf1 = new Configuration();
        conf1.set("textinputformat.record.delimiter", ".");

        Job NgramJob = Job.getInstance(conf1);
        NgramJob.setJarByClass(Driver.class);

        NgramJob.setMapperClass(NgramBuilder.NgramMapper.class);
        NgramJob.setReducerClass(NgramBuilder.NgramReducer.class);

        NgramJob.setOutputKeyClass(Text.class);
        NgramJob.setOutputValueClass(IntWritable.class);

        NgramJob.setInputFormatClass(TextInputFormat.class);
        NgramJob.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(NgramJob, new Path(inputPath));
        TextOutputFormat.setOutputPath(NgramJob, new Path(outputPath));

        NgramJob.waitForCompletion(true);
    }

}