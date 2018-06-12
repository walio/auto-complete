package NgramWithSmoothing.ExtractKgram;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class ExtractCount {
    public static class ExtractMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        int Gramlength;
        @Override
        public void setup(Context context){
            Gramlength = context.getConfiguration().getInt("GramLength",4);
        }

        private Text word=new Text();
        private IntWritable count = new IntWritable(1);
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString().trim();
            String[] words = line.split("\\s+");

            if(words.length!=Gramlength+1){
                return;
            }

            StringBuilder sb = new StringBuilder(words[0]);
            for (int i=1;i<Gramlength;++i){
                sb.append(" ");
                sb.append(words[i]);
            }
            count.set(Integer.parseInt(words[words.length-1]));
            word.set(sb.toString());
            context.write(word, count);

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

    public static void extract(String inputPath,String outputPath,int order) throws Exception{
        Configuration conf = new Configuration();
        conf.setInt("GramLength",order);

        Job job = Job.getInstance(conf);
        job.setJarByClass(ExtractCount.class);

        job.setMapperClass(ExtractCount.ExtractMapper.class);
        job.setReducerClass(ExtractCount.ExtractReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job, new Path(inputPath));
        TextOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }
}
