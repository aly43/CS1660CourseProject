
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class Search {
	
	private static String searchTerm;
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        searchTerm = otherArgs[2];
        conf.set("search", searchTerm);
        // if less than two paths 
        // provided will show error
        if (otherArgs.length < 2) 
        {
            System.err.println("Error: please provide two paths");
            System.exit(2);
        }
        
        Job job = Job.getInstance(conf, "search term");
        job.setJarByClass(Search.class);
        job.setMapperClass(searchMapper.class);
        job.setReducerClass(searchReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

	public static class searchMapper extends Mapper<Object, Text, Text, LongWritable>{

	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	String[] tokens = value.toString().split("\t");
			
			String word = tokens[0];
			String doc = tokens[1];
			long count = Long.parseLong(tokens[2]);
			
			context.write(new Text(word + "\t" + doc), new LongWritable(count));
	    }
	}
	 
	 public static class searchReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
		 
		 private HashMap<String, Long> results;
		 private TreeMap<Text, LongWritable> tmap;
		 
		 @Override
		 public void setup(Context context) throws IOException, InterruptedException
			{				
			 	Configuration conf = context.getConfiguration();
				searchTerm = conf.get("search");
				results = new HashMap<String, Long>();
				tmap = new TreeMap<Text,LongWritable>();
			}

		 @Override
		 public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

			 	String[] tokens = key.toString().split("\t");
	    		
			 
			 	String word = tokens[0];
			 	String doc = tokens[1];
			 	long count = 0;
			 
		        for (LongWritable value : values) {
		            
					count = (value.get());
		        	
		        }
		        
		        results.put(word + "\t" + doc, count);
		        
		        // Write to context 
		        for (HashMap.Entry<String,Long> entry : results.entrySet()) {
		            tmap.put(new Text(entry.getKey()), new LongWritable(entry.getValue()));
		        }


		 }
		 
		 @Override
		 public void cleanup(Context context) throws IOException, InterruptedException
		    {
			 	for (Map.Entry<Text,LongWritable> entry : tmap.entrySet()) {
			 		String[] tokens = entry.toString().split("\t");
			 		if (tokens[0].equals(searchTerm)) {
			 			context.write(new Text(entry.getKey()), (entry.getValue()));
			 		}
				}
		    }

	 }
	 

}
