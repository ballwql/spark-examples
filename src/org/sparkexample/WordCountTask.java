package org.sparkexample;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Iterator;


import scala.Tuple2;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * WordCountTask class, we will call this class with the test WordCountTest.
 */
public class WordCountTask {
  /**
   * We use a logger to print the output. Sl4j is a common library which works with log4j, the
   * logging system used by Apache Spark.
   */
  //private static final Logger LOGGER = LoggerFactory.getLogger(WordCountTask.class);

  public static void main(String[] args)  {
    checkArgument(args.length > 1, "Please provide the path of input file as first parameter,the path of output file as second parameter.");
    String inputFilePath=args[0];
    String outputFilePath=args[1];
    //String inputFilePath="/tmp/input/t1.txt"
    //String outputFilePath="/tmp/input/t2.txt"
    /*
     * This is the address of the Spark cluster. We will call the task from WordCountTest and we
     * use a local standalone cluster. [*] means use all the cores available.
     * See {@see http://spark.apache.org/docs/latest/submitting-applications.html#master-urls}.
     */
    //String master = "local[*]";
    String master = "spark://10-163-161-230:7077";

    /*
     * Initialises a Spark context.
     */
    SparkConf conf = new SparkConf()
        .setAppName(WordCountTask.class.getName())
        .setMaster(master);
    JavaSparkContext context = new JavaSparkContext(conf);
    /*
     * Performs a work count sequence of tasks and prints the output with a logger.
     */
    JavaRDD<String> input = context.textFile(inputFilePath);
    JavaRDD<String> words = input.flatMap(
     new FlatMapFunction<String,String>(){
        @Override 
        public Iterator<String> call(String x){
            return Arrays.asList(x.split(" ")).iterator();
        }});
    //transfer into word and count.
    JavaPairRDD<String,Integer> counts = words.mapToPair(
        new PairFunction<String,String,Integer>(){
        @Override
        public Tuple2<String,Integer> call(String x){
          return new Tuple2(x,1);
        }}).reduceByKey(
        new Function2<Integer,Integer,Integer>(){
        @Override
	public Integer call(Integer x,Integer y){ return x+y;}});
   
    counts.saveAsTextFile(outputFilePath);
  }
}
