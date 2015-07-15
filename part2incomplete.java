/*
  Code copied from https://github.com/facebookarchive/hadoop-20/blob/master/src/examples/org/apache/hadoop/examples/WordCount.java
*/

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.lang.Float;

public class WordCount {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable>{
           // DoubleWritable wri = new DoubleWritable();
    		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      			DoubleWritable wri = new DoubleWritable();
                StringTokenizer itr = new StringTokenizer(value.toString(), ",");
      			itr.nextToken();
			int i = 1;
			while (itr.hasMoreTokens()) {
				Text t1 = new Text("gene_" + i);
			    double val = Double.parseDouble(itr.nextToken());
                wri.set(val);
				context.write(t1, wri);
				i++;
      			}
    		}	
  	}
  
    public static class IntSumReducer 
       extends Reducer<Text,FloatWritable,Text,FloatWritable> {

    public void reduce(Text key, Iterable<FloatWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
        int count = 0;
        int sizeCount = 0;
        for (FloatWritable val : values) {
	        sizeCount++;
            if (val > 0.5)
                count++;
        }


      // context.write(key, result);
    }
  } 

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
