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
public class Covid19_2{
	public static class MyMapper extends Mapper<Object, Text, Text, LongWritable> {
		private static LongWritable number;
		private Text word = new Text();
		private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		
		private Date start;
		private Date end;
		private Date dt;
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			try{
 				start = formatter.parse(conf.get("StartDate"));
				end = formatter.parse(conf.get("EndDate"));
				}catch(ParseException excpt){
					excpt.printStackTrace();		
				}
			
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
				dt = formatter.parse(date);
				}catch(ParseException excpt){
					excpt.printStackTrace();		
				}
				if(dt.getTime()<=end.getTime()&&dt.getTime()>=start.getTime()){
					String coun = tok.nextToken();
					String newCase = tok.nextToken();
					String deathCase = tok.nextToken();
					word.set(coun);
					number = new LongWritable(Integer.valueOf(deathCase));
					context.write(word,number);
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
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		Date sDate = formatter.parse("1970-01-01");
		Date eDate = formatter.parse("1970-01-01");
		Date earliestDate = formatter.parse("1970-01-01");
		Date latestDate = formatter.parse("1970-01-01");
		try{
			sDate = formatter.parse(args[1]);
			eDate = formatter.parse(args[2]);
			earliestDate = formatter.parse("2019-12-31");
			latestDate = formatter.parse("2020-04-08");
		}catch(Exception ex){
			System.out.println("Invalid Date");
			System.exit(1);
			ex.printStackTrace();		
		}
		
		if(eDate.before(sDate)){
			System.out.println("Invalid Date");
			System.exit(1);
		}

		if(sDate.before(earliestDate)||latestDate.before(sDate)||eDate.before(earliestDate)||latestDate.before(eDate)){
			System.out.println("Invalid Date");
			System.exit(1);
		}
		
		conf.set("StartDate",args[1]);
		conf.set("EndDate",args[2]);

		Job myjob = Job.getInstance(conf, "Covid19_2");
		myjob.setJarByClass(Covid19_2.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(LongWritable.class);
		// Uncomment to set the number of reduce tasks
		// myjob.setNumReduceTasks(2);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob,  new Path(args[3]));
		long endTime = System.currentTimeMillis();
 		System.out.println("Covid19_2 running time "+(endTime-startTime)+" ms");
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
	}
}
