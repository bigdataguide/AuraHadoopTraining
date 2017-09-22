package org.training.hadoop.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.InputSplit;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

public class InvertIndex {

  public static class IndexMapper extends Mapper<Object, Text, Text, Text> {
    private Text word = new Text();
    private Text fileName = new Text();
    public void map(Object key,
                    Text value,
                    Context context) throws IOException, InterruptedException {
      // TODO please implement this method
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());


        InputSplit inputSplit = context.getInputSplit();
        String dirName = ((FileSplit) inputSplit).getPath().getParent().getName();
        String fName = ((FileSplit) inputSplit).getPath().toString();
        fileName.set(dirName+"/"+fName);
        context.write(word, fileName);
      }
    }
  }

  public static class IndexCombiner extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key,
                       Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {
      // TODO please implement this method
      StringBuffer fileTmp=new StringBuffer();
      for (Text val : values) {
        if(fileTmp.indexOf(val.toString())<0){
          fileTmp.append(val.toString()+"--");
        }else{
          System.out.println(key.toString()+"====IndexCombiner====="+val.toString());
        }

      }
      context.write(key,new Text(fileTmp.toString()));
    }
  }

  public static class IndexReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key,
                       Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {
      // TODO please implement this method
      StringBuffer fileTmp=new StringBuffer();
      for (Text val : values) {
        if(fileTmp.indexOf(val.toString())<0){

          fileTmp.append(val.toString()+"--");
        }else{
          System.out.println(key.toString()+"====reduce====="+val.toString());
        }
      }
      context.write(key,new Text(fileTmp.toString()));
    }
  }


  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: invertindex <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "Invert Index");
    // TODO please set correct mapper and reducer for this class as well as output key/value class
    job.setJarByClass(InvertIndex.class);
    job.setMapperClass(IndexMapper.class);
    job.setCombinerClass(IndexCombiner.class);
    job.setReducerClass(IndexReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setNumReduceTasks(2);


    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
        new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
