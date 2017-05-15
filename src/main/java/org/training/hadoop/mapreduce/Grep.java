package org.training.hadoop.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.map.RegexMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.UUID;

public class Grep extends Configured implements Tool {

  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: Grep <input> <output> <regex> [<group>]");
      ToolRunner.printGenericCommandUsage(System.out);
      System.exit(2);
    }
    Configuration conf = getConf();
    conf.set(RegexMapper.PATTERN, args[2]);
    if (args.length == 4) {
      conf.set(RegexMapper.GROUP, args[3]);
    }

    Path tmpDir = new Path("/tmp/grep-tmp" + UUID.randomUUID().toString());
    try {
      Job grepJob = Job.getInstance(conf);
      FileInputFormat.addInputPath(grepJob, new Path(args[0]));
      FileSystem fs = FileSystem.get(conf);
      if (fs.exists(tmpDir)) {
        fs.delete(tmpDir, true);
      }
      FileOutputFormat.setOutputPath(grepJob, tmpDir);

      grepJob.setJobName("Grep-grep");
      grepJob.setJarByClass(Grep.class);
      grepJob.setMapperClass(RegexMapper.class);
      grepJob.setReducerClass(LongSumReducer.class);
      grepJob.setOutputKeyClass(Text.class);
      grepJob.setOutputValueClass(LongWritable.class);
      grepJob.setOutputFormatClass(SequenceFileOutputFormat.class);

      grepJob.waitForCompletion(true);

      Job sortJob = Job.getInstance(conf);
      Path output = new Path(args[1]);
      if (fs.exists(output)) {
        fs.delete(output, true);
      }
      FileInputFormat.addInputPath(sortJob, tmpDir);
      FileOutputFormat.setOutputPath(sortJob, output);

      sortJob.setJobName("Grep-sort");
      sortJob.setJarByClass(Grep.class);
      sortJob.setInputFormatClass(SequenceFileInputFormat.class);
      sortJob.setMapperClass(InverseMapper.class);
      sortJob.setNumReduceTasks(1);
      sortJob.setSortComparatorClass(LongWritable.DecreasingComparator.class);

      return sortJob.waitForCompletion(true) ? 0 : 1;
    } finally {
      FileSystem.get(conf).delete(tmpDir, true);
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Grep(), args);
    System.exit(res);
  }
}

