package org.sparkexample;
import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.DoubleFunction;
public class Avg {
	public static void printAvg1(JavaSparkContext sc){
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4));
    	//map: function applied each element of rdd, return new rdd with new elements
    	JavaRDD<Integer> result = rdd.map(new Function<Integer,Integer>(){
    		public Integer call(Integer x){
    			return x*x;
    		}
    	});
    	System.out.println(StringUtils.join(result.collect(),","));
	}
	public static void printAvg2(JavaSparkContext sc){
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4));
		JavaDoubleRDD result = rdd.mapToDouble(
			new DoubleFunction<Integer>(){
				public double call(Integer x){
					return (double)x*x;
				}});
		System.out.println(result.mean());
			
	}
    public static void main(String[] args){
    	String master="spark://10-163-161-230:7077";
    	SparkConf conf = new SparkConf().setAppName(Avg.class.getName()).setMaster(master);
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	printAvg1(sc);
    	printAvg2(sc);
    	
    }
}
