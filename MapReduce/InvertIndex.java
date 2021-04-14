
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class InvertIndex {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		//conf.set("mapreduce.output.textoutputformat.separator","\t");
        Job job = Job.getInstance(conf, "inverted_index");
        job.setJarByClass(InvertIndex.class);
        job.setMapperClass(invertedIndexMapper.class);
        job.setReducerClass(invertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

	public static class invertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>{

		private Text docID = new Text();
	    private Text word = new Text();
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        // Reading input one line at a time and tokenizing
	        String line = value.toString().replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase();

	        StringTokenizer tokenizer = new StringTokenizer(line);

	        // Determine what document we are in
	        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	        docID.set(fileName);

	        // Iterating through all of the words available in that line and forming the key value pair
	        while (tokenizer.hasMoreTokens()) {
	            word.set(tokenizer.nextToken());
	            context.write(word, docID);
	        }
	    }
	}
	 
	 public static class invertedIndexReducer extends Reducer<Text,Text,Text,Text> {

		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			 HashMap<String, Integer> results = new HashMap<String, Integer>();
		        // Iterate through the values available with a key and add them together to give final result as the key and sum of its values
		        for (Text value : values) {
		            int count;
		            if(results.containsKey(value.toString())){
		            	count = results.get(value.toString());
		            } 
		            else{
		            	count = 0;
		            }
		            results.put(value.toString(), count + 1);
		        }
		        // Write to context 
		        for (HashMap.Entry<String,Integer> entry : results.entrySet()) {
		            context.write(key, new Text(entry.getKey() + "\t" + entry.getValue()));
		        }

		 }

	 }
	 

}
