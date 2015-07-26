/*
  Code copied from https://github.com/facebookarchive/hadoop-20/blob/master/src/examples/org/apache/hadoop/examples/WordCount.java
*/

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.lang.Float;
import java.util.ArrayList;
import java.util.*;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

public class Part3 extends Configured implements Tool{

  public static class GeneMapper extends Mapper<Object, Text, Text, Text>{
           	//DoubleWritable wri = new DoubleWritable();
           	private String sample;
           	private float val;
            private Text t1 = new Text();
           	private Text pair = new Text();
  		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      			//DoubleWritable wri = new DoubleWritable();
                StringTokenizer itr = new StringTokenizer(value.toString(), ",");
      			sample= itr.nextToken();
				int i = 1;
				while (itr.hasMoreTokens()) {
          t1.set("gene_" + i);
          val = Float.parseFloat(itr.nextToken());
          if(val > 0.000001){
            pair.set(sample+"-"+Float.toString(val));
            context.write(t1, pair);
          }
          i++;
        }
  		}	
  	}
    
    public static class MultCombiner
       extends Reducer<Text,Text,Text,Text> {
        Text samplePair = new Text();
        Text product = new Text();
        ArrayList<String> geneSamples;
        //Text newKey = new Text();
    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
        geneSamples = new ArrayList<String>();
        String sample1;
        String sample2;
        int sampleVal1;
        int sampleVal2;
        float exprVal1;
        float exprVal2;
        String[] split1;
        String[] split2;
        String base = "";
        float mult;
        for (Text val : values) {
	        geneSamples.add(val.toString());
        }
        for(int i = 0; i<geneSamples.size(); i++){
          split1 = geneSamples.get(i).split("-");
          sample1 = split1[0];
          exprVal1 = Float.parseFloat(split1[1]);
          sampleVal1 = Integer.parseInt(sample1.split("_")[1]);
          for(int j = 0; j<geneSamples.size();j++){
            split2 = geneSamples.get(j).split("-");
            sample2 = split2[0];
            exprVal2 = Float.parseFloat(split2[1]);
            sampleVal2 = Integer.parseInt(sample2.split("_")[1]);
            if(sampleVal1 < sampleVal2){
              samplePair.set(sample1+","+sample2);
              mult = exprVal1*exprVal2;
              product.set(Float.toString(mult));
              context.write(samplePair,product);
            }
          }
          //base+=geneSamples.get(i);
        }
        //context.write(key,new Text(base));
    }
  }
  public static class SecondMapper extends Mapper<Object, Text, Text, Text>{
           Text samplePair = new Text();
           Text val = new Text();
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
            samplePair.set(itr.nextToken());
            val.set(itr.nextToken());
            context.write(samplePair,val);
    }
  }
  public static class MultReducer
       extends Reducer<Text,Text,Text,FloatWritable> {
        FloatWritable total = new FloatWritable();
    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
        float exprProduct;
        float sum = 0;
        for (Text val : values) {
          exprProduct = Float.parseFloat(val.toString());
          sum+= exprProduct;
        }
        total.set(sum);
        context.write(key,total);
    }
  }
  /*public static void main(String[] args) throws Exception {
    final String OUTPUT_PATH = "intermediate_output";
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator",",");
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "part3 matrix");
    job.setJarByClass(Part3.class);
    job.setMapperClass(GeneMapper.class);
    //job.setCombinerClass(MultCombiner.class);
    job.setReducerClass(MultCombiner.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    //job.setInputFormatClass(FileInputFormat.class);
  //job.setOutputFormatClass(TextOutputFormat.class);
    // Configuration reduceConf = new Configuration(false);
    // ChainMapper.addMapper(job,GeneMapper.class,Object.class,
    //   Text.class,Text.class,Text.class,reduceConf);
    // Configuration mapBConf = new Configuration(false);
    // ChainMapper.addMapper(job,MultMapper.class,Text.class,
    //   Text.class,Text.class,Text.class,mapBConf);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
    job.waitForCompletion(true);

    Configuration conf2 = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator",",");
    Job job2 = new Job(conf2, "Job 2");
    job2.setJarByClass(Part3.class);
    job2.setMapperClass(SecondMapper.class);
    job2.setReducerClass(MultReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    //job.setInputFormatClass(TextInputFormat.class);
  //job.setOutputFormatClass(FileOutputFormat.class);

  TextInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
  FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }*/



 private static final String OUTPUT_PATH = "intermediate_output";

 @Override
 public int run(String[] args) throws Exception {
  boolean end = false;
  /*
   * Job 1
   */
  Configuration conf = getConf();
  FileSystem fs = FileSystem.get(conf);
  Job job = new Job(conf, "Job1");
  job.setJarByClass(Part3.class);

  job.setMapperClass(GeneMapper.class);
  job.setReducerClass(MultCombiner.class);

  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(Text.class);

  job.setInputFormatClass(TextInputFormat.class);
  job.setOutputFormatClass(TextOutputFormat.class);

  TextInputFormat.addInputPath(job, new Path(args[0]));
  TextOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

  job.waitForCompletion(true);

  /*
   * Job 2
   */
  Configuration conf2 = getConf();
  conf2.set("mapreduce.output.textoutputformat.separator",",");
  Job job2 = new Job(conf2, "Job 2");
  job2.setJarByClass(Part3.class);

  job2.setMapperClass(SecondMapper.class);
  job2.setReducerClass(MultReducer.class);

  job2.setMapOutputKeyClass(Text.class);
  job2.setMapOutputValueClass(Text.class);
  job2.setOutputKeyClass(Text.class);
  job2.setOutputValueClass(FloatWritable.class);

  job2.setInputFormatClass(TextInputFormat.class);
  job2.setOutputFormatClass(TextOutputFormat.class);

  TextInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
  TextOutputFormat.setOutputPath(job2, new Path(args[1]));
  end = job2.waitForCompletion(true);
  if(end){
    fs.delete(new Path(OUTPUT_PATH));
  }
  return end ? 0 : 1;
 }

 /**
  * Method Name: main Return type: none Purpose:Read the arguments from
  * command line and run the Job till completion
  * 
  */
 public static void main(String[] args) throws Exception {
  // TODO Auto-generated method stub
  if (args.length != 2) {
   System.err.println("Enter valid number of arguments <Inputdirectory>  <Outputlocation>");
   System.exit(0);
  }
  ToolRunner.run(new Configuration(), new Part3(), args);
 }
}