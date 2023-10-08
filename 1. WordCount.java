import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, IntWritable, Text >{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	StringTokenizer itr = new StringTokenizer(value.toString());
    	String aux = "";
    	int i = 0;
        while (itr.hasMoreTokens()) {
        	
        	aux += itr.nextToken(); // Concat word into string
        	aux += " ";
        	i++;
        	if(i >= 2){
        		word.set(aux);
            	context.write(one, word); // Write out each pair (word, num)
				aux = "";
            	i=0;
        	}
        }
        
    }
  }

  public static class IntSumReducer
       extends Reducer<IntWritable, Text, IntWritable, Text> {
    private Text result = new Text();

    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
	  int highest = 0;
	  for (Text val : values) {
		String aux = val.toString();
		String[] arr = aux.split(" "); // Split word/num pair
		int times = Integer.parseInt(arr[1]); // Get word count
		if(times > highest){
			highest = times;
			result.set(val);
	    }
	  }
	  context.write(key, result); // Write out highest
    }
  }
  
  public static void main(String[] args) throws Exception {
	System.out.println (">>>>>>>> Master-SPAI (ALEX) >>>>>>>>>");
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count v1.0");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
