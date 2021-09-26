package main;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import twitter_kafka.TwitterKafkaProducer;




public class App {
	public static void main(String[] args) throws IOException {
		
		 System.out.println("TwitterKafkaProducerApp is running");
	        TwitterKafkaProducer producer = new TwitterKafkaProducer();
	        producer.run();

	}

}
