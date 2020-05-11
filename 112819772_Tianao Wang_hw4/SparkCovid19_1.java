/* Java imports */
import java.util.*;
import java.lang.Iterable;
import java.util.Iterator;
import java.text.SimpleDateFormat;
import java.text.ParseException;

/* Spark imports */
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
public class SparkCovid19_1 {

    public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
    /**
     * args[0]: Input file path on distributed file system
     * args[1]: Output file path on distributed file system
     * @throws 
     */
    public static void main(String[] args) throws ParseException{
    	long startTime = System.currentTimeMillis();
	String input = args[0];
	final String StartDate = args[1];
	final String EndDate = args[2];
	String output = args[3];
	
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
	
	
	
	/* essential to run any spark code */
	SparkConf conf = new SparkConf().setAppName("SparkCovid19_1").setMaster("local");
	JavaSparkContext sc = new JavaSparkContext(conf);

	/* load input data to RDD */
	JavaRDD<String> dataRDD = sc.textFile(input);

	JavaPairRDD<String, Integer> counts =
	    dataRDD.flatMapToPair(new PairFlatMapFunction<String, String, Integer>(){
		    public Iterator<Tuple2<String, Integer>> call(String value) throws ParseException{
		   
			Date start = formatter.parse(StartDate);
			Date end = formatter.parse(EndDate);
			Date dt;
			List<Tuple2<String, Integer>> retWords =
			     	new ArrayList<Tuple2<String, Integer>>();
			String[] lines = value.split(",");
			if(lines[0].equals("date")){
				
			}else{
				dt = formatter.parse(lines[0]);
				if(dt.getTime()<=end.getTime()&&dt.getTime()>=start.getTime()){
					String coun = lines[1];
					String deathCase = lines[3];
					retWords.add(new Tuple2<String, Integer>(coun, Integer.valueOf(deathCase)));
				}
				
			}
			
			return retWords.iterator();
		    }
		}).reduceByKey(new Function2<Integer, Integer, Integer>(){
			public Integer call(Integer x, Integer y){
			    return x+y;
			}
		    });
	
//	counts.sortByKey(new Comparator<String>(){
//		@Override
//		public int compare(String arg0, String arg1) {
//			if(arg0.compareTo(arg1)<0){
//				return -1;
//			}else if(arg0.compareTo(arg1)==0){
//				return 0;
//			}else{
//				return 1;
//			}
//		}
//		
//	},true);
	counts.saveAsTextFile(output);
	long endTime = System.currentTimeMillis();
 	System.out.println("sparkCovid19_1 running time "+(endTime-startTime)+" ms");
    }
}
