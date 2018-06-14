package NgramWithSmoothing.Calculation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CalculateDriver {
    public static void calculateProb(String inputPath,String outputPath,String lowerOrderInputPath,double discount) throws Exception{

        //calculate lambda,λ(w_(history))
        Configuration conf = new Configuration();
        conf.setDouble("discount",discount);

        Job LambdaJob = Job.getInstance(conf);
        LambdaJob.setJarByClass(Summation.class);

        LambdaJob.setMapperClass(Lambda.LambdaMapper.class);
        LambdaJob.setReducerClass(Lambda.LambdaReducer.class);

        LambdaJob.setOutputKeyClass(Text.class);
        LambdaJob.setOutputValueClass(DoubleWritable.class);

        LambdaJob.setInputFormatClass(TextInputFormat.class);
        LambdaJob.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(LambdaJob, new Path(inputPath));
        TextOutputFormat.setOutputPath(LambdaJob, new Path(outputPath+"Lambda"));

        LambdaJob.waitForCompletion(true);

        //multiply lambda and lower prob
        Job MultiplicationJob = Job.getInstance(conf);
        MultiplicationJob.setJarByClass(Summation.class);

        MultiplicationJob.setReducerClass(Multiplication.MultiplicationReducer.class);

        MultiplicationJob.setOutputKeyClass(Text.class);
        MultiplicationJob.setOutputValueClass(DoubleWritable.class);

        MultiplicationJob.setMapOutputValueClass(Text.class);

        MultipleInputs.addInputPath(MultiplicationJob, new Path(outputPath+"Lambda"), TextInputFormat.class, Multiplication.LambdaMapper.class);
        MultipleInputs.addInputPath(MultiplicationJob, new Path(lowerOrderInputPath), TextInputFormat.class, Multiplication.LowerProbMapper.class);

        TextOutputFormat.setOutputPath(MultiplicationJob, new Path(outputPath+"Interpolation"));

        MultiplicationJob.waitForCompletion(true);

        //calculate higher Prob term
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

        //sum higher Prob and Interpolation
        Job SummationJob = Job.getInstance(conf);
        SummationJob.setJarByClass(Summation.class);

        SummationJob.setMapperClass(Summation.SummationMapper.class);
        SummationJob.setReducerClass(Summation.SummationReducer.class);

        SummationJob.setOutputKeyClass(Text.class);
        SummationJob.setOutputValueClass(DoubleWritable.class);

        SummationJob.setInputFormatClass(TextInputFormat.class);
        SummationJob.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(SummationJob, new Path(outputPath+"Interpolation"));
        TextInputFormat.addInputPath(SummationJob, new Path(outputPath+"HigherProb"));
        TextOutputFormat.setOutputPath(SummationJob, new Path(outputPath));

        SummationJob.waitForCompletion(true);
    }
}
