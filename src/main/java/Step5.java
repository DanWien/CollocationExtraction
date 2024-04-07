import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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

public class Step5 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length == 3) {
                String decade = parts[0];
                String bigram = parts[1];
                String npmi = parts[2];
                if (Double.parseDouble(npmi) < 1)
                    context.write(new Text(decade + "\t" + npmi), new Text(bigram));
            }
        }
    }



    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] parts = key.toString().split("\t");
            String decade = parts[0];
            String npmi = parts[1];
            for (Text bigram : values) {
                context.write(new Text(decade + "\t" + bigram.toString()), new Text(npmi));
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

    public static class NPMIDescendingComparator extends WritableComparator {
        protected NPMIDescendingComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            Text k1 = (Text) w1;
            Text k2 = (Text) w2;
            String[] parts1 = k1.toString().split("\t");
            String[] parts2 = k2.toString().split("\t");

            int decadeComparison = parts1[0].compareTo(parts2[0]);
            if (decadeComparison != 0) {
                return decadeComparison;
            } else {
                Double npmi1 = Double.parseDouble(parts1[1]);
                Double npmi2 = Double.parseDouble(parts2[1]);
                return -npmi1.compareTo(npmi2);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 5 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(32 * 1024 * 1024));
        Job job = Job.getInstance(conf, "Step 5");
        job.setJarByClass(Step5.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(DecadePartitioner.class);
        job.setSortComparatorClass(NPMIDescendingComparator.class);
//        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job, new Path("s3://this-is-my-first-bucket-dn2/step4_output"));
        FileOutputFormat.setOutputPath(job, new Path("s3://this-is-my-first-bucket-dn2/step5_output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

