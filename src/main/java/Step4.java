import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class Step4 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length == 3) {
                String decade = parts[0];
                String bigram = parts[1];
                double npmi = Double.parseDouble(parts[2]);

                context.write(new Text(decade + "\t" + bigram), new DoubleWritable(npmi));

                context.write(new Text(decade + "\t*"), new DoubleWritable(npmi));
                }
            }
        }



    public static class ReducerClass extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private double minPmi;
        private double relMinPmi;
        private double totalNpmi;
        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            this.minPmi = Double.parseDouble(conf.get("minPmi"));
            this.relMinPmi = Double.parseDouble(conf.get("relMinPmi"));
            this.totalNpmi = 0;
        }

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            String[] parts = key.toString().split("\t");
            if (parts[1].equals("*")) {
                double sum = 0;
                for(DoubleWritable val : values)
                    sum += val.get();
                totalNpmi = sum;
            }
            else {
                boolean isCollocation = false;
                double currNpmi = 0;
                for (DoubleWritable val : values){
                    currNpmi= val.get();
                    isCollocation = (currNpmi >= minPmi) || (currNpmi/totalNpmi >= relMinPmi);
                }
                if (isCollocation){
                    context.write(key, new DoubleWritable(currNpmi));
                }

            }
        }
    }


    public static class DecadePartitioner extends Partitioner<Text, DoubleWritable> {
        @Override
        public int getPartition(Text key, DoubleWritable value, int numPartitions) {
            String[] parts = key.toString().split("\t");
            String decade = parts[0];
            return (decade.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 4 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        String minPmi = args[1];
        String relMinPmi = args[2];
        System.out.println("minPmi = " + minPmi + ", relMinPmi = " + relMinPmi);
        Configuration conf = new Configuration();
        conf.set("minPmi", minPmi);
        conf.set("relMinPmi", relMinPmi);
        conf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(32 * 1024 * 1024));
        Job job = Job.getInstance(conf, "Step 4");
        job.setJarByClass(Step4.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(DecadePartitioner.class);
//        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);



        FileInputFormat.addInputPath(job, new Path("s3://this-is-my-first-bucket-dn2/step3_output"));
        FileOutputFormat.setOutputPath(job, new Path("s3://this-is-my-first-bucket-dn2/step4_output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

