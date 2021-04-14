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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TopN {
	private static String topNumber;

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        topNumber = otherArgs[2];
        conf.set("number", topNumber);
        // if less than two paths 
        // provided will show error
        if (otherArgs.length < 2) 
        {
            System.err.println("Error: please provide two paths");
            System.exit(2);
        }
  
        Job job = Job.getInstance(conf, "top n");
        job.setJarByClass(TopN.class);
  
        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);
  
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
  
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        
        job.setNumReduceTasks(1);
  
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
  
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static class TopNMapper extends Mapper<Object, Text, LongWritable, Text> {

		
		@Override
		public void map(Object key, Text value,Context context) throws IOException, InterruptedException
		{
		
			// input data format => movie_name    
			// no_of_views  (tab seperated)
			// we split the input data
			String[] tokens = value.toString().split("\t");
		
			String word = tokens[0];
			long count = Long.parseLong(tokens[2]);
			
			// insert data into treeMap,
			// we want top 10  viewed movies
			// so we pass no_of_views as key
//			if (tmap.containsValue(word)) {
//				tmap.put(count+, word)
//			}
			
			count = (-1) * count;
			
			context.write(new LongWritable(count), new Text(word));

		}
		
	}
	
	public static class TopNReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	
		static int count;
		private TreeMap<String, Long> tmap;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			String param = conf.get("number");
			count = Integer.parseInt(param);
			
			tmap = new TreeMap<String, Long>(Collections.reverseOrder());
		}
		
		@Override
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
		
			// input data from mapper
			// key                values
			// movie_name         [ count ]
			String name = null;
			long num = (-1)*key.get();
			
			for (Text val : values)
			{
				name = val.toString();
			}
			
//			for(Map.Entry<Long, String> entry: tmap2.entrySet()) {
//				if(entry.getValue().toString())
//			}
			if (tmap.containsKey(name))
			{
				tmap.put(name, tmap.get(name) + num);
			}
			else
			{
				tmap.put(name, num);
			}
			
			// we remove the first key-value
			// if it's size increases 10
//			if (count > 0)
//			{
//				if (tmap.get(num) == null)
//				{
//					tmap.put(num, name);
//				}
//				else {
//					for(Map.Entry<Long, String> entry: tmap.entrySet())
//					{
//						if (entry.getValue().equals(name)) {
//							long temp = entry.getKey() + num;
//							
//							tmap.put(temp, name);
//							
//							tmap.remove(entry.getKey(),entry.getValue());
//							count++;
//						}
//						
//					}
//				}
//				count--;
//			}
		}
		
		@Override
	    public void cleanup(Context context) throws IOException, InterruptedException
	    {
			Map sort = valueSort(tmap);
			Set set = sort.entrySet();
			Iterator i = set.iterator();
	  
	        while(i.hasNext()) {
	        	Map.Entry me = (Map.Entry)i.next();
	        	if (count > 0) {
		        	context.write(new LongWritable((long) me.getKey()), new Text((String) me.getValue()));	
		        	count--;
	        	}
	        }
	    }

		private Map valueSort(TreeMap<String, Long> tmap2) {
			// TODO Auto-generated method stub
			TreeMap<Long, String> flip = new TreeMap<Long, String>(Collections.reverseOrder());
			for (Map.Entry<String, Long> entry: tmap2.entrySet())
			{
				flip.put(entry.getValue(), entry.getKey());
			}
			
			return flip;
		}
		
	}

}
