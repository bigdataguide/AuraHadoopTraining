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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;



/***
 *
 * 按照年龄分组，并降序输出所有人成绩
 * select * from student order by age,score;
 * */
public class AgePartitionOrderBy extends Configured implements Tool {

  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: AgePartitionOrderBy <input> <output>");
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
    job.setJarByClass(AgePartitionOrderBy.class);
    job.setMapperClass(AgeMapperOrderBy.class);
    job.setPartitionerClass(AgePartitionerOrderBy.class);
    job.setReducerClass(AgeReducerOrderBy.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setNumReduceTasks(3);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new AgePartitionOrderBy(), args);
    System.exit(res);
  }
}

class AgePartitionerOrderBy extends Partitioner<Text, Text> {

  @Override
  public int getPartition(Text key, Text value, int n) {
    if (n == 0) {
      return 0;
    }

    int age = Integer.parseInt(key.toString());
    if (age <= 20) {
      return 0;
    } else if (age <= 50) {
      return 1 % n;
    } else {
      return 2 % n;
    }
  }
}

class AgeMapperOrderBy extends Mapper<Object, Text, Text, Text> {

  private Text outKey = new Text();
  private Text outValue = new Text();

  @Override
  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    // name, age, gender, score
    String[] arr = value.toString().split(",");
    outKey.set(arr[1]); // age
    outValue.set(arr[0] + "," + arr[2] + "," + arr[3]); // name,gender,score
    context.write(outKey, outValue);
  }
}

class AgeReducerOrderBy extends Reducer<Text, Text, Text, Text> {
  private Text outKey = new Text();
  private Text outValue = new Text();

  private String name;
  private String gender;
  private int maxScore;

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    int age = Integer.parseInt(key.toString());;
    maxScore =0;

    List<Student> scoreList=new ArrayList<Student>();

    for (Text t : values) {

      String[] arr = t.toString().split(",");
      int score = Integer.parseInt(arr[2]);
      name = arr[0];
       gender = (arr[1]);
      Student student=new Student();
      student.setAge(age);
      student.setGender(gender);
      student.setName(name);
      student.setScore(score);
      scoreList.add(student);
    }


    while(scoreList.size()>0){

      int maxIndex=0;
      for(int k=1;k<scoreList.size();k++){
          if(scoreList.get(k).getScore()>scoreList.get(maxIndex).getScore())   {

            maxIndex=k;
          };
      }

      Student s=scoreList.get(maxIndex);
      outKey.set(s.getName()); // name
      outValue.set("age-" + s.getAge() + ",gender-" + s.getGender() + ",score-" + s.getScore());
      context.write(outKey, outValue);
      scoreList.remove(maxIndex);

    }

    //sort by scores排序，对value
//    for(int j=0;j<scoreListOrdered.size();j++){
//      System.out.println("=============="+scoreListOrdered.get(j).getScore()+scoreListOrdered.get(j).getName());
//      Student s=scoreListOrdered.get(j);
//      outKey.set(s.getName()); // name
//      outValue.set("age-" + s.getAge() + ",gender-" + s.getGender() + ",score-" + s.getScore());
//      context.write(outKey, outValue);
//    }

  }
}
