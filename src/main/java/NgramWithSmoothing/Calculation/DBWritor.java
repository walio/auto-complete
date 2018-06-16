package NgramWithSmoothing.Calculation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

public class DBWritor {
    public static class DBMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text KeyOut = new Text();
        private Text ValueOut = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString().trim();
            String[] words = line.split("\\s+");

            StringBuilder sb = new StringBuilder();
            for (int i=0;i<words.length-2;++i){
                sb.append(" ");
                sb.append(words[i]);
            }
            KeyOut.set(sb.toString().trim());
            ValueOut.set(words[words.length-2]+"="+words[words.length-1]);

            context.write(KeyOut,ValueOut);

        }

    }

    public static class DBReducer extends TableReducer<Text, Text, NullWritable> {

        private DoubleWritable ValueOut = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //todo:is it necessary to put only top k?
            for (Text value:values){
                String[] s = value.toString().split("=");
                Put put = new Put(key.getBytes());
                put.addColumn(Bytes.toBytes("content"),Bytes.toBytes(s[0]),Bytes.toBytes(s[1]));
                context.write(NullWritable.get(),put);
            }
        }
    }

    public static void writeDB(String inputPath) throws Exception{
        //write to hbase
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "Master");
        conf.set(TableOutputFormat.OUTPUT_TABLE, "PredictWords");

        Job job = Job.getInstance(conf);
        TableMapReduceUtil.addDependencyJars(job);
        job.setJarByClass(DBWritor.class);

        job.setMapperClass(DBWritor.DBMapper.class);
        job.setReducerClass(DBWritor.DBReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TableOutputFormat.class);

        TextInputFormat.setInputPaths(job, inputPath);

        job.waitForCompletion(true);
    }
}
