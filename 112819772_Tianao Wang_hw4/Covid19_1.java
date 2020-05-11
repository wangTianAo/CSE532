import java.io.IOException;
import java.util.*;
import java.text.SimpleDateFormat;
import java.text.ParseException;

import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class Covid19_1{
	public static class MyMapper extends Mapper<Object, Text, Text, LongWritable> {
		private static LongWritable number;
		private Text word = new Text();
		private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		
		private Date DateBegin;
		private Date dt;
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			boolean WorldFlag = Boolean.valueOf(conf.get("WorldF")).booleanValue();
			//if(key!=null){
			StringTokenizer tok = new StringTokenizer(value.toString(),",");
			String date = "";
			while(tok.hasMoreTokens()){
				date = tok.nextToken();
				if(date.equals("date")){
				tok.nextToken();
				tok.nextToken();
				tok.nextToken();				
				}
				else{		
				try{
 				DateBegin = formatter.parse("2020-01-01");
				dt = formatter.parse(date);
				}catch(ParseException excpt){
					excpt.printStackTrace();		
				}
				if(dt.before(DateBegin)){
					String coun = tok.nextToken();
					String newCase = tok.nextToken();
					String deathCase = tok.nextToken();
				}
				else{
					String coun = tok.nextToken();
					String newCase = tok.nextToken();
					String deathCase = tok.nextToken();
					if(WorldFlag){
					word.set(coun);
					number = new LongWritable(Integer.valueOf(newCase));
					context.write(word,number);
					}else{
						if(!coun.equals("World")){
					word.set(coun);
					number = new LongWritable(Integer.valueOf(newCase));
					context.write(word,number);
						}
					}

				}

			}
			//}
			

			}
			
		}
		
	}
	
	
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable total = new LongWritable();
		
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable tmp: values) {
				sum += tmp.get();
			}
			total.set(sum);
			// This write to the final output
			context.write(key, total);
		}
	}
	
	
	public static void main(String[] args)  throws Exception {
		long startTime = System.currentTimeMillis();
		Configuration conf = new Configuration();
		conf.set("WorldF",args[1]);
		Job myjob = Job.getInstance(conf, "Covid19_1");
		myjob.setJarByClass(Covid19_1.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(LongWritable.class);
		// Uncomment to set the number of reduce tasks
		// myjob.setNumReduceTasks(2);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob,  new Path(args[2]));
		long endTime = System.currentTimeMillis();
 		System.out.println("Covid19_1 running time "+(endTime-startTime)+" ms");
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
	}
}
