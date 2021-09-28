package com.habtom.finalproject3.streaming.SparkStreamingApp;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

public class KafkaSparkStreamingAPP {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setAppName("kafkaSparkStreamingAPP").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(20000));

		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", "localhost:9092");
		Set<String> topics = Collections.singleton("tweets");

		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

		  List<String> allRecord = new ArrayList<String>();
		  final String COMMA = ",";
		  
		  directKafkaStream.foreachRDD(rdd -> {
			  
		  System.out.println("New data arrived  " + rdd.partitions().size() +" Partitions and " + rdd.count() + " Records");
			  if(rdd.count() > 0) {
				rdd.collect().forEach(rawRecord -> {
					  
					  System.out.println(rawRecord);
					  System.out.println("***************************************");
					  System.out.println(" Streaming ......."+rawRecord._2);
					  System.out.println("***************************************");
					  String record = rawRecord._2();
					  StringTokenizer st = new StringTokenizer(record,",");
					  
					  
					  System.out.println("All records ********** :"+allRecord.toString()); 
				  });
				
				
				System.out.println("Streaming done : ");
				
			  }
		  });
		 
		ssc.start();
		ssc.awaitTermination();
	}
}
