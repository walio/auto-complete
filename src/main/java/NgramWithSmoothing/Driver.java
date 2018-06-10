package NgramWithSmoothing;

import NgramWithSmoothing.Continuation.ContinuationDriver;
import NgramWithSmoothing.HighestOrderProb.HighestOrderProb;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class Driver {
    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();

        //NgramJob
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

        TextInputFormat.setInputPaths(NgramJob, new Path(args[0]));
        TextOutputFormat.setOutputPath(NgramJob, new Path("NgramLibrary"));

        NgramJob.waitForCompletion(true);

        //highest order term,(C{w(i−1)wi}−d)/C(wi−1)
        Configuration NullConf = new Configuration();

        Job HighestOrderJob = Job.getInstance(NullConf);
        HighestOrderJob.setJarByClass(Driver.class);

        HighestOrderJob.setMapperClass(HighestOrderProb.HighestOrderProbMapper.class);
        HighestOrderJob.setReducerClass(HighestOrderProb.HighestOrderProbReducer.class);

        HighestOrderJob.setOutputKeyClass(Text.class);
        HighestOrderJob.setOutputValueClass(DoubleWritable.class);

        HighestOrderJob.setMapOutputValueClass(Text.class);

        HighestOrderJob.setInputFormatClass(TextInputFormat.class);
        HighestOrderJob.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(HighestOrderJob, new Path("NgramLibrary"));
        TextOutputFormat.setOutputPath(HighestOrderJob, new Path("HighestOrderProb"));

        HighestOrderJob.waitForCompletion(true);

        //continuation
        for(int i = 3;i<4;++i){
            ContinuationDriver.main(new String[]{String.valueOf(i),"0.75"});
        }


    }
}
