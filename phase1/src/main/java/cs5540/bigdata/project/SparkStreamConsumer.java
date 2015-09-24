package cs5540.bigdata.project;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

//This code was developed with the help of the Spark Streaming documentation
//introduction found at:
//https://spark.apache.org/docs/1.1.0/streaming-programming-guide.html

public class SparkStreamConsumer {
	public static void main(String[] args){
		run();
	}

	public static void run(){
		//Create a local spark streaming context
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Twitter Stream Processor");
		JavaStreamingContext jsc = new JavaStreamingContext(conf, new Duration(1000));
		
		//Create DStream (Discretized Stream) that will connect to host
		//and port
		JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 9999);
		
		//Split each line into words
		JavaDStream<String> words = lines.flatMap(
			new FlatMapFunction<String, String>(){
				@Override
				public Iterable<String> call(String x){
					return Arrays.asList(x.split(" "));
				}
			}
		);
		
		//Count each word in each batch
		JavaPairDStream<String, Integer> pairs = words.map(
			new PairFunction<String, String, Integer>(){
				@Override
				public Tuple2<String, Integer> call(String s) throws Exception {
					return new Tuple2<String, Integer>(s, 1);
				}
			}
		);

		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
			new Function2<Integer, Integer, Integer>(){
				@Override
				public Integer call(Integer i1, Integer i2) throws Exception {
					return i1 + i2;
				}
			}
		);

		//Print the results
		wordCounts.print();

		//Now that computation is set up, invoke
		jsc.start();
		jsc.awaitTermination();
	}
}
