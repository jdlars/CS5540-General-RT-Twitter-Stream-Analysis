package cs5540.bigdata.project;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

public class DataAnalyzer {
	public static void main(String[] args){
		countWords();
	}

	public static void countWords(){
		SparkConf config = new SparkConf().setMaster("local").setAppName("Twitter Stream WordCount");
		JavaSparkContext sc = new JavaSparkContext(config);
	
		// 0. Establish connection to in/out files
		String inputFile = "input.txt";
		String outputFile = "output.txt";

		// Load our input data.
		JavaRDD<String> input = sc.textFile(inputFile);
		// Split up into words.
		JavaRDD<String> words = input.flatMap(
		  new FlatMapFunction<String, String>() {
		    public Iterable<String> call(String x) {
		      return Arrays.asList(x.split(" "));
		    }});
		// Transform into pairs and count.
		JavaPairRDD<String, Integer> counts = words.mapToPair(
		  new PairFunction<String, String, Integer>(){
		    public Tuple2<String, Integer> call(String x){
		      return new Tuple2(x, 1);
		    }}).reduceByKey(new Function2<Integer, Integer, Integer>(){
		        public Integer call(Integer x, Integer y){ return x + y;}});
		// Save the word count back out to a text file, causing evaluation.
		counts.saveAsTextFile(outputFile);
		
//		// 1. Load the input file
//		JavaRDD<String> input = sc.textFile(inputFile);
//	
//		// 2. Split up input data into words
//		JavaRDD<String> words = input.flatMap(
//			new FlatMapFunction<String, String>() {
//				public Iterable<String> call(String x){
//					return Arrays.asList(x.split(" "));
//				}
//			}
//		);
//
//		// 3. Transform info pairs and count words
//		JavaPairRDD<String, Integer> counts = words.mapToPair(
//			new PairFunction<String, String, Integer>(){
//				public Tuple2<String, Integer> call(String x){
//					return new Tuple2(x, 1);
//				}
//			}
//		).reduceByKey(new Function2<Integer, Integer, Integer>(){
//			public Integer call(Integer x, Integer y){return x + y;}
//		});
//	
//		// 4. Save the word count back to output file resulting in evaluation
//		// of the function
//		counts.saveAsTextFile(outputFile);

		// 5. Close output streams		
	}
} 

