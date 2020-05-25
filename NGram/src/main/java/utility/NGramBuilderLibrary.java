import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NGramLibraryBuilder {
    public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        int noGram;
        @Override
        public void setup(Context context) {
            //get n-gram from command line through context configuration
            Configuration conf = context.getConfiguration();
            noGram = conf.getInt("noGram",5); // default noGram is 5 (this is to avoid if any failure happens with any other portions of the projects)
        }


        /**
         * mapper read in text line by line
         *  input:
         *  i love big data
         *  i love
         *  {"i love", [1,1]}
         * output:
         * {"i love", 1}
         * {"i love", 1}
         * {"love big", 1}
         * {"big data", 1}
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            line = line.trim().toLowerCase();

            //how to remove useless elements?
            line = line.replaceAll("[^a-z]","");

            //how to separate word by space?
            String [] words = line.split("\\s+");
            if (words.length < 2) {return;}

            int arraylength = words.length;
            StringBuilder sb;
            for (int i=0;i<arraylength-1;i++){
                sb=new StringBuilder();
                sb.append(words[i]);
                for (n=1;n<noGram && i+n<arraylength;n++){
                    sb.append(" ");
                    sb.append(words[i+n]);
                    context.write(new Text(sb.toString().trim()),new IntWritable(1));
                }
            }
        }
    }

    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        /**
         * reducer that takes in the pair inputkey - list of occurence, and writes to a hdfs file with inputkey - sum of occurence
         * input:
         * {"i love", [1,1]}
         * {"love big", 1}
         * {"big data", 1}
         * output:
         * {"i love", 2}
         * {"love big", 1}
         * {"big data", 1}
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value:values){
                sum += value.get();
            }
            context.write(key,new IntWritable(sum));
            //context write back to hdfs

        }
    }

}