package NgramWithSmoothing.Continuation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class ContinuationDriver {
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
                context.getCounter("gram","total").increment(1);
                context.write(value, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception{

        int N = Integer.parseInt(args[0]);
        double d = Double.parseDouble(args[1]);

        //extract only Ngram
        Configuration conf = new Configuration();
        conf.setInt("GramLength",N);

        Job ExtractJob = Job.getInstance(conf);
        ExtractJob.setJarByClass(ContinuationDriver.class);

        ExtractJob.setMapperClass(ContinuationDriver.ExtractMapper.class);

        ExtractJob.setMapOutputKeyClass(Text.class);
        ExtractJob.setMapOutputValueClass(NullWritable.class);

        ExtractJob.setInputFormatClass(TextInputFormat.class);
        ExtractJob.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(ExtractJob, new Path("NgramLibrary"));
        TextOutputFormat.setOutputPath(ExtractJob, new Path(String.valueOf(N)+"gramOnlyWords"));

        ExtractJob.waitForCompletion(true);

        //lambda,λ(w_(history))
        Configuration confd = new Configuration();
        confd.setDouble("d",d);

        Job LambdaJob = Job.getInstance(confd);
        LambdaJob.setJarByClass(ContinuationDriver.class);

        LambdaJob.setMapperClass(Lambda.LambdaMapper.class);
        LambdaJob.setReducerClass(Lambda.LambdaReducer.class);

        LambdaJob.setOutputKeyClass(Text.class);
        LambdaJob.setOutputValueClass(Text.class);

        LambdaJob.setInputFormatClass(TextInputFormat.class);
        LambdaJob.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(LambdaJob, new Path(String.valueOf(N)+"gramOnlyWords"));
        TextOutputFormat.setOutputPath(LambdaJob, new Path(String.valueOf(N)+"gramOnlyLambda"));

        LambdaJob.waitForCompletion(true);

        //continuation prob,P(now)
        confd.setLong("total",ExtractJob.getCounters().findCounter("gram","total").getValue());
        Job ProbJob = Job.getInstance(confd);
        ProbJob.setJarByClass(ContinuationDriver.class);

        ProbJob.setMapperClass(Probability.ProbMapper.class);
        ProbJob.setReducerClass(Probability.ProbReducer.class);

        ProbJob.setOutputKeyClass(Text.class);
        ProbJob.setOutputValueClass(Text.class);

        ProbJob.setInputFormatClass(TextInputFormat.class);
        ProbJob.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(ProbJob, new Path(String.valueOf(N)+"gramOnlyWords"));
        TextOutputFormat.setOutputPath(ProbJob, new Path(String.valueOf(N)+"gramOnlyProb"));

        ProbJob.waitForCompletion(true);

        //multiplication P(now)*λ(history)

        Configuration NullConf = new Configuration();

       Job multiJob = Job.getInstance(NullConf);
        multiJob.setJarByClass(ContinuationDriver.class);

        multiJob.setMapperClass(Multiplication.TermMapper.class);
        multiJob.setReducerClass(Multiplication.MultiReducer.class);

        multiJob.setOutputKeyClass(Text.class);
        multiJob.setOutputValueClass(DoubleWritable.class);

        multiJob.setInputFormatClass(TextInputFormat.class);
        multiJob.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(multiJob,new Path(String.valueOf(N)+"gramOnlyLambda"));
        TextInputFormat.addInputPath(multiJob,new Path(String.valueOf(N)+"gramOnlyProb"));
        TextOutputFormat.setOutputPath(multiJob, new Path(String.valueOf(N)+"gramContinuation"));

        multiJob.waitForCompletion(true);
    }
}
