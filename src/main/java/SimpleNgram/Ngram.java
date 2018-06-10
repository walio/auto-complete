package SimpleNgram;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Ngram {

    public static class NgramMapper extends Mapper<Object, Text, Text, IntWritable>{

        int Ng;
        @Override
        public void setup(Context context){
            Ng = context.getConfiguration().getInt("Ng",5);
        }

        private IntWritable one = new IntWritable(1);
        private Text word=new Text();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");

            for(int i=0;i<words.length-1;++i){
                StringBuilder sb = new StringBuilder();
                for(int corlen=0;corlen<Ng;++corlen){
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


}