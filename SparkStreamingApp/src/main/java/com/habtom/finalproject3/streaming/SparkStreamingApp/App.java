package com.habtom.finalproject3.streaming.SparkStreamingApp;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) throws Exception {

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(
				"kafkastreamingAPP");
		JavaStreamingContext jssc = new JavaStreamingContext(conf,
				Durations.seconds(5));

		Map<String, Integer> topicPartitionMap = new HashMap<>();
		topicPartitionMap.put("tweets", 1);

		JavaPairDStream<String, String> s = KafkaUtils.createStream(jssc,
				"localhost:6181", "test", topicPartitionMap);
		s.map(x -> x._2).dstream()
				.saveAsTextFiles("1-spark-streaming-output/record", null);

//		Map<String, String> kafkaParams = new HashMap<>();
//		
//		Set<String> topics = Collections.singleton("tweets");
//		 
//		kafkaParams.put("bootstrap.servers", "localhost:9092");
//		kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		kafkaParams.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		kafkaParams.put("auto.offset.reset", "latest");
//		 kafkaParams.put("group.id", "GRP2");
//		 kafkaParams.put("enable.auto.commit", "false");
//		 
//		 JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(jssc, String.class,
//					String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
//		 
//		 
//		 directKafkaStream.foreachRDD(rdd -> {
//			  
//			  System.out.println("New data arrived  " + rdd.partitions().size() +" Partitions and " + rdd.count() + " Records");
//		 });

		
		
		
		
		jssc.start();
		jssc.awaitTermination();
		
	}
}
