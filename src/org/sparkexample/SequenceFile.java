package org.sparkexample;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import java.util.Map;

public class SequenceFile {
	
	public static class ConvertToNativeTypes implements 
		PairFunction<Tuple2<Text,IntWritable>,String,Integer>{
			public Tuple2<String,Integer> call(Tuple2<Text,IntWritable> record){
				return new Tuple2(record._1.toString(),record._2.get());
			}
	}
	public static void main(String[] args){
		String fileName=args[0];
		String master="spark://10-163-161-230:7077";
    	SparkConf conf = new SparkConf().setAppName(Avg.class.getName()).setMaster(master);
    	JavaSparkContext sc = new JavaSparkContext(conf);
		JavaPairRDD<Text,IntWritable> input = sc.sequenceFile(fileName,Text.class,IntWritable.class);
		JavaPairRDD<String,Integer> result = input.mapToPair(new ConvertToNativeTypes());
		Map<String,Integer> strmap = result.collectAsMap();
		for(Map.Entry<String,Integer> entry: strmap.entrySet()){
			System.out.println(entry.getKey()+":"+entry.getValue());
		}
	}
	

}
