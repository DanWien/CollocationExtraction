import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.InputStream;
import java.net.URI;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;


import java.io.IOException;
import java.util.StringTokenizer;

public class Step1 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Set<String> stopWords = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            InputStream is = getClass().getClassLoader().getResourceAsStream("eng-stopwords.txt");
            BufferedReader br = new BufferedReader(new InputStreamReader(is));

            String line;
            while ((line = br.readLine()) != null) {
                stopWords.add(line.trim());
            }
            br.close();
        }


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length >= 4) {
                String biGram = parts[0];
                String year = parts[1];
                int count = Integer.parseInt(parts[2]);

                String decade = year.substring(0, year.length() - 1) + "0";

                String[] words = biGram.split("\\s+");
                if (words.length == 2 &&
                        !stopWords.contains(words[0]) && !stopWords.contains(words[1]) &&
                        words[0].matches("^[a-zA-Z]+$") && words[1].matches("^[a-zA-Z]+$")) {

                    context.write(new Text(decade + "\t" + biGram), new IntWritable(count));

                    context.write(new Text(decade + "\t*"), new IntWritable(count));
                }
            }
        }
    }


    public static class ReducerClass extends Reducer<Text,IntWritable,Text,Text> {
        private int n = 0;
        private String decade = "";

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            String[] parts = key.toString().split("\t");
            if (!decade.equals(parts[0])) {
                decade = parts[0];
                if(parts[1].equals("*"))
                    n = sum;
            }
            else {
                double partialPMI = Math.log(sum) + Math.log(n);
                double den = Math.log(n) - Math.log(sum);
                context.write(key, new Text(sum + "\t" + partialPMI + "\t" + den));
            }
        }
    }

    public static class CombinerClass extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }


    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            String[] parts = key.toString().split("\t");
            String decade = parts[0];
            return (decade.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(32 * 1024 * 1024));
        Job job = Job.getInstance(conf, "Step 1");
        job.setJarByClass(Step1.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
//        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/2gram/data"));

        FileOutputFormat.setOutputPath(job, new Path("s3://this-is-my-first-bucket-dn2/step1_output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
