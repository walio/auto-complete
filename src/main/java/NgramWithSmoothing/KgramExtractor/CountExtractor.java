package NgramWithSmoothing.KgramExtractor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class CountExtractor {
    public static class ExtractMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        int Gramlength;
        @Override
        public void setup(Context context){
            Gramlength = context.getConfiguration().getInt("GramLength",4);
        }

        private Text word=new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString().trim();
            String[] words = line.split("\\s+");

            if(words.length==Gramlength+1){
                word.set(line);
                context.write(word,NullWritable.get());
            }

        }
    }

    public static void extract(String inputPath,String outputPath,int order) throws ClassNotFoundException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setInt("GramLength",order);

        Job job = Job.getInstance(conf);
        job.setJarByClass(CountExtractor.class);

        job.setMapperClass(CountExtractor.ExtractMapper.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

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
