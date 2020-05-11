/* Java imports */
import java.util.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.Iterable;
import java.net.URI;
import java.nio.file.FileSystem;
import java.util.Iterator;
import java.text.SimpleDateFormat;
import java.text.ParseException;

/* Spark imports */
import scala.Tuple2;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
public class SparkCovid19_2 {
	static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	static Map<String,Double> map = new Hashtable<String,Double>();
    /**
     * args[0]: Input file path on distributed file system
     * args[1]: Output file path on distributed file system
     * @throws 
     */
    public static void main(String[] args) throws ParseException{
    long startTime = System.currentTimeMillis();
	String input = args[0];
	String popPath = args[1];
	String output = args[2];
	
	double total;
			
	/* essential to run any spark code */
	SparkConf conf = new SparkConf().setAppName("SparkCovid19_2").setMaster("local");
	JavaSparkContext sc = new JavaSparkContext(conf);
	
	//System.out.println(popPath);
	JavaRDD<String> dataPop = sc.textFile(popPath);
	JavaPairRDD<String,Double> popTemp =
			dataPop.flatMapToPair(new PairFlatMapFunction<String, String, Double>(){
			    public Iterator<Tuple2<String, Double>> call(String value) throws ParseException{
				//System.out.println(value);
			    List<Tuple2<String, Double>> result =
				     	new ArrayList<Tuple2<String, Double>>();
				String[] lines = value.split(",");
				if(lines[0].equals("countriesAndTerritories")){
					
				}else{
					if(lines.length==5){
						String coun = lines[1].trim();
						Double pop = Double.valueOf(lines[4].trim());
						if(pop>0){
							//map.put(coun,pop);
							result.add(new Tuple2<String, Double>(coun, Double.valueOf(pop)));
						}			
						
					}
				}	
				return result.iterator();
			    }
			});
	map = popTemp.collectAsMap();
	/* load input data to RDD */
	JavaRDD<String> dataRDD = sc.textFile(input);

	JavaPairRDD<String, Double> counts =
	    dataRDD.flatMapToPair(new PairFlatMapFunction<String, String, Double>(){
		    public Iterator<Tuple2<String, Double>> call(String value) throws ParseException{
			Date dt;
			List<Tuple2<String, Double>> retWords =
			     	new ArrayList<Tuple2<String, Double>>();
			String[] lines = value.split(",");
			if(lines[0].equals("date")){
				
			}else{
				final String aa = lines[0];
				dt = formatter.parse(aa);
				String coun = lines[1];
				String newCase = lines[2];
				double totalC = Double.valueOf(newCase.toString());
				Double result = 0.0;
				if( map.get(coun)!=null){
				double pop = map.get(coun);
				result = new Double((totalC/pop)*1000000);
				retWords.add(new Tuple2<String, Double>(coun, result));
				}
				
			}
			
			return retWords.iterator();
		    }
		}).reduceByKey(new Function2<Double, Double, Double>(){
			public Double call(Double x, Double y){
			    return x+y;
			}
		    });
	
	counts.saveAsTextFile(output);
	long endTime = System.currentTimeMillis();
    System.out.println("sparkCovid19_2 running time "+(endTime-startTime)+" ms");
    }
    
}

