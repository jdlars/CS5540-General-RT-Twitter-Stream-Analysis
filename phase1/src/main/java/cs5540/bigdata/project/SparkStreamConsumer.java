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
import org.apache.spark.streaming.twitter.*;
import twitter4j.*;
import scala.Tuple2;

//This code was developed with the help of the Spark Streaming documentation
//introduction found at:
//https://spark.apache.org/docs/1.1.0/streaming-programming-guide.html

public class SparkStreamConsumer {
	public static void main(String[] args){
		run();
	}

	public static void run(){

		//Tokens
		String CONSUMER_KEY = "DsrKVsGSct5khALPJJh3VM7aZ";
		String CONSUMER_SECRET = "SCrcpDiw5Pw7mKqBBn6f4cLw2I4Fhxss1mI8eWLp5kRJy9LWcP";
		String ACCESS_TOKEN = "3633627554-rFls2gI4qbrqJTbR0BZKoCADouY2TkjLFEa8FQe";
		String ACCESS_TOKEN_SECRET = "GBgogkDScpUknkuXrl4nNrQhEPIW65TJlkRkX3rkBHqO2";

		//Set twitter4j oauth properties
		System.setProperty("twitter4j.oauth.consumerKey", CONSUMER_KEY);
		System.setProperty("twitter4j.oauth.consumerSecret", CONSUMER_SECRET);
		System.setProperty("twitter4j.oauth.accessToken", ACCESS_TOKEN);
		System.setProperty("twitter4j.oauth.accessTokenSecret", ACCESS_TOKEN_SECRET);

		//Create a local spark streaming context
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Twitter Stream Processor");
		JavaStreamingContext jsc = new JavaStreamingContext(conf, new Duration(1000));
		
		//Create DStream (Discretized Stream) of twitter user statuses
		JavaReceiverInputDStream<Status> statuses = TwitterUtils.createStream(jsc);
		
		//Split each line into words
		JavaDStream<String> words = statuses.flatMap(
			new FlatMapFunction<Status, String>(){
				@Override
				public Iterable<String> call(Status x){
					return Arrays.asList(x.getText().split(" "));
				}
			}
		);
		
		//Count each word in each batch
		JavaPairDStream<String, Integer> pairs = words.mapToPair(
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
