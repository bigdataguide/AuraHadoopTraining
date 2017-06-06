package org.training.hadoop.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class AgePartition extends Configured implements Tool {

  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: AgePartition <input> <output>");
      ToolRunner.printGenericCommandUsage(System.out);
      System.exit(2);
    }

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    Path output = new Path(args[1]);
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(output)) {
      fs.delete(output, true);
    }
    FileOutputFormat.setOutputPath(job, output);
    job.setJarByClass(AgePartition.class);
    job.setMapperClass(AgeMapper.class);
    job.setPartitionerClass(AgePartitioner.class);
    job.setReducerClass(AgeReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setNumReduceTasks(3);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new AgePartition(), args);
    System.exit(res);
  }
}

class AgePartitioner extends Partitioner<Text, Text> {

  @Override
  public int getPartition(Text key, Text value, int n) {
    if (n == 0) {
      return 0;
    }
    String[] arr = value.toString().split(",");
    int age = Integer.parseInt(arr[1]);
    if (age <= 20) {
      return 0;
    } else if (age <= 50) {
      return 1 % n;
    } else {
      return 2 % n;
    }
  }
}

class AgeMapper extends Mapper<Object, Text, Text, Text> {

  private Text outKey = new Text();
  private Text outValue = new Text();

  @Override
  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    // name, age, gender, score
    String[] arr = value.toString().split(",");
    outKey.set(arr[2]); // gender
    outValue.set(arr[0] + "," + arr[1] + "," + arr[3]); // name,age,score
    context.write(outKey, outValue);
  }
}

class AgeReducer extends Reducer<Text, Text, Text, Text> {
  private Text outKey = new Text();
  private Text outValue = new Text();

  private String name;
  private int age;
  private int maxScore = 0;

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    String gender = key.toString();
    maxScore = 0;
    for (Text t : values) {
      String[] arr = t.toString().split(",");
      int score = Integer.parseInt(arr[2]);
      if (score > maxScore) {
        name = arr[0];
        age = Integer.parseInt(arr[1]);
        maxScore = score;
      }
    }
    outKey.set(name); // name
    outValue.set("age-" + age + ",gender-" + gender + ",maxScore-" + maxScore);
    context.write(outKey, outValue);
  }
}
