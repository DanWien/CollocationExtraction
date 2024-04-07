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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import java.net.URI;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;


import java.io.IOException;
import java.util.StringTokenizer;

public class Step2 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length >= 5) {
                String decade = parts[0];
                String bigram = parts[1];
                String count = parts[2];
                String partialPMI = parts[3];
                String den = parts[4];

                context.write(new Text(decade + "\t" + bigram), new Text(count + "\t" + partialPMI + "\t" + den));

                String[] words = bigram.split("\\s+");
                if (words.length == 2) {
                    context.write(new Text(decade + "\t" + words[0] + "\t*"), new Text(count));
                }
            }
        }
    }


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private int sumCount;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String[] keyParts = key.toString().split("\t");
            if(keyParts.length == 3) {
                sumCount = 0;
                for (Text val : values) {
                    sumCount += Integer.parseInt(val.toString());
                }
            }
            else {
                for(Text val:values) {
                    String[] valParts = val.toString().split("\t");
                    double partialPMI = Double.parseDouble(valParts[1]);
                    partialPMI -= Math.log(sumCount);
                    context.write(key,new Text(valParts[0] + "\t" + partialPMI + "\t" + valParts[2]));
                }
            }
        }
    }


    public static class DecadePartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String[] parts = key.toString().split("\t");
            String decade = parts[0];
            return (decade.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(32 * 1024 * 1024));
        Job job = Job.getInstance(conf, "Step 2");
        job.setJarByClass(Step2.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(DecadePartitioner.class);
//        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job, new Path("s3://this-is-my-first-bucket-dn2/step1_output"));
        FileOutputFormat.setOutputPath(job, new Path("s3://this-is-my-first-bucket-dn2/step2_output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
