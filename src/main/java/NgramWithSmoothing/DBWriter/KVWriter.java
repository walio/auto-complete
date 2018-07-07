package NgramWithSmoothing.DBWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

public class KVWriter {
    public static class writerMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

        private Text KeyOut = new Text();
        private Text ValueOut = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\\s+");

            StringBuilder sb = new StringBuilder();
            for (int i=0;i<words.length-1;++i){
                sb.append(" ");
                sb.append(words[i]);
            }

            Put put = new Put(sb.toString().trim().getBytes());
            put.addColumn("lambda".getBytes(),"lambda".getBytes(),words[words.length-1].getBytes());

            context.write(null,put);
        }
    }

    public static void write(String inputPath, String outputTable) throws ClassNotFoundException, IOException, InterruptedException{
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "47.93.218.135");
        conf.set(TableOutputFormat.OUTPUT_TABLE, outputTable);

        Job job = Job.getInstance(conf);
        job.setJarByClass(KVWriter.class);

        job.setMapperClass(writerMapper.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TableOutputFormat.class);

        TextInputFormat.setInputPaths(job, new Path(inputPath));

        job.waitForCompletion(true);
    }
    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        write(args[0],args[1]);
    }
}