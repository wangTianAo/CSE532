import java.io.IOException;
import java.util.*;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.Hashtable;
import java.net.URI;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Covid19_3{
	public static class MyMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		private static DoubleWritable number;
		private Text word = new Text();
		private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		
		private Date start;
		private Date end;
		private Date dt;
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{	
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
				String coun = tok.nextToken();
				String newCase = tok.nextToken();
				String deathCase = tok.nextToken();

				word.set(coun);
				number = new DoubleWritable(Double.valueOf(newCase));
				context.write(word,number);			
				}

			}
			//}
			

			}
			
		
		
	}
	
	
	public static class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable total = new DoubleWritable();
		private Map<String,Double> map = new Hashtable<String,Double>();		
		
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (DoubleWritable tmp: values) {
				sum += tmp.get();
			}
			total.set(sum);
			// This write to the final output
			double totalC = Double.valueOf(total.toString());
			if( map.get(key.toString())!=null){
			double pop = map.get(key.toString());
			DoubleWritable result = new DoubleWritable((totalC/pop)*1000000);
			context.write(key, result);
			}
			
		}

		public void setup(Context context) throws IOException,FileNotFoundException{
			Configuration conf = context.getConfiguration();
			URI[] files = context.getCacheFiles();
			for(URI file:files){
			BufferedReader br = null;
			FSDataInputStream fsr = null;
			FileSystem fs = FileSystem.get(URI.create(file.toString()),conf);
			fsr = fs.open(new Path(file.toString()));
			br = new BufferedReader(new InputStreamReader(fsr));				
			String line = null;
			while((line = br.readLine())!=null){
			//StringTokenizer tok = new StringTokenizer(line,",");
			String[] stringsplit = line.split(",");
			if(stringsplit.length!=5){
				continue;
			}
			if(stringsplit[0].equals("countriesAndTerritories")){
				continue;			
			}else{
				String coun = stringsplit[1].trim();
				Double pop = Double.valueOf(stringsplit[4].trim());
				if(pop>0){
					map.put(coun,pop);
				}			
			}
				//while(tok.hasMoreTokens()){
				//String first = tok.nextToken();
				//if(first.equals("countriesAndTerritories")){
				//	tok.nextToken();
				//	tok.nextToken();
				//	tok.nextToken();
				//	tok.nextToken();
				//}else{
				//	String coun = tok.nextToken();
				//	tok.nextToken();
				//	tok.nextToken();
				//	Double pop = Double.valueOf(tok.nextToken());
				//	if(pop>0){
				//		map.put(coun,pop);
				//	}	
				//}
				
				//}	
				}
			}		
		}
	}
	
	
	public static void main(String[] args)  throws Exception {
		long startTime = System.currentTimeMillis();
		Configuration conf = new Configuration();
		//conf.set("CachePath",new Path(args[0]));
		Job myjob = Job.getInstance(conf, "Covid19_3");
		myjob.addCacheFile(new Path(args[1]).toUri());
		myjob.setJarByClass(Covid19_3.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(DoubleWritable.class);
		// Uncomment to set the number of reduce tasks
		// myjob.setNumReduceTasks(2);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob,  new Path(args[2]));
		long endTime = System.currentTimeMillis();
 		System.out.println("Covid19_3 running time "+(endTime-startTime)+" ms");
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
	}
}
