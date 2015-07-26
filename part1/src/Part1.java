/*
  Code copied from https://github.com/facebookarchive/hadoop-20/blob/master/src/examples/org/apache/hadoop/examples/WordCount.java
*/

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.util.GenericOptionsParser;
import java.lang.Float;
import java.util.ArrayList;
import java.util.*;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

public class Part1 {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, ArrayWritable>{
   Log log = LogFactory.getLog(TokenizerMapper.class);
    private Text sample = new Text();
    private ArrayList<Float> exprVals; //list of all the expr values per sample
    private ArrayList<String> maxVals; //list of the max expr vals
    private TextArrayWritable results;
    private float max;
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(),",");
      sample.set(itr.nextToken()); //set key as first value in string as sample
      exprVals = new ArrayList<Float>();
      maxVals = new ArrayList<String>();
      results = new TextArrayWritable();
      max = 0f;
      while (itr.hasMoreTokens()) {
      	exprVals.add(Float.parseFloat(itr.nextToken()));
      }
      //find max
      log.info(sample.toString());
      for(int i = 0; i< exprVals.size(); i++){
      	if( exprVals.get(i) > max){
      		max = exprVals.get(i);
      	}
      }
      //add max values to a list
      for(int j = 0; j<exprVals.size(); j++){
      	if(Math.abs(max -(exprVals.get(j))) < 0.00001){
      		maxVals.add("gene_"+Integer.toString(j+1));
      	}
      }
      //add the max values to the result 
      results = new TextArrayWritable(maxVals.toArray(new String[maxVals.size()]));
      context.write(sample,results);
    }
  }
  //implement the TextArrayWritable
  public static class TextArrayWritable extends ArrayWritable{
  	public TextArrayWritable(){
  		super(Text.class);
  	}
  	public TextArrayWritable(String[] strings){
  		super(Text.class);
  		Text[] texts = new Text[strings.length];
  		for(int i = 0; i<strings.length; i++){
  			texts[i] = new Text(strings[i]);
  		}
  		set(texts);
  	}

    @Override
  	public String toString() {
  	  String line = Arrays.toString(get());
  	  return line.substring(1,line.length()-1);
  	}
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator",",");
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "part1 max gene vals");
    job.setJarByClass(Part1.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(TextArrayWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
