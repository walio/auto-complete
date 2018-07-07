package NgramWithSmoothing.DBWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.PriorityQueue;

public class ProbWriter {
    public static class writerMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text KeyOut = new Text();
        private Text ValueOut = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\\s+");

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

    static class Pair{
        String key;
        Double value;
        Pair(String key, Double value){
            this.key=key;
            this.value=value;
        }
    }
    public static class writerReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            PriorityQueue<Pair> pq = new PriorityQueue<Pair>(10, (pair1, pair2) ->{
                if (pair1.value==pair2.value){
                    return 0;
                }else if (pair1.value>pair2.value){
                    return -1;
                }else{
                    return 1;
                }
            });
            Put put;
            if (key.toString().equals("")){
                put = new Put(" ".getBytes());
            }else {
                put = new Put(key.toString().getBytes());
            }

            for (Text value:values){
                String[] twi = value.toString().split("=");
                pq.add(new Pair(twi[0],Double.parseDouble(twi[1])));
            }
            int i=0;
            for(Pair pair:pq){
                ++i;
                put.addColumn("predict".getBytes(),String.format("%010d",i).getBytes(),(pair.key+"="+pair.value).getBytes());
            }
            context.write(null,put);
        }
    }

    public static void write(String inputPath, String outputTable) throws ClassNotFoundException, IOException, InterruptedException{
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "47.93.218.135");
        conf.set(TableOutputFormat.OUTPUT_TABLE, outputTable);

        Job job = Job.getInstance(conf);
        job.setJarByClass(ProbWriter.class);

        job.setMapperClass(writerMapper.class);
        job.setReducerClass(writerReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TableOutputFormat.class);

        TextInputFormat.setInputPaths(job, new Path(inputPath));

        job.waitForCompletion(true);
    }
    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        write(args[0],args[1]);
    }
}
