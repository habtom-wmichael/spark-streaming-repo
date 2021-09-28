package com.f.pro.hab.SpSqlhbas.main;


import java.io.IOException;
import java.util.*;

import kafka.serializer.StringDecoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
//import org.apache.spark.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.*;

import com.f.pro.hab.SpSqlhbas.pojo.Tweet;
import com.google.gson.Gson;

import scala.Tuple2;


public class KafkaSparkStreamingHbaseApp 
{
	static Configuration config;
	static Job newAPIJobConfiguration;
	
    @SuppressWarnings("deprecation")
	public static void main( String[] args )
    {
        System.out.println( "starting spark steaming!" );
        SparkConf sconf = new SparkConf().setAppName("KafkaStream").setMaster("local[*]");
		sconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sconf.registerKryoClasses(new Class[]{org.apache.hadoop.hbase.io.ImmutableBytesWritable.class});
        JavaSparkContext jsc = new JavaSparkContext(sconf);
    	JavaStreamingContext ssc = new JavaStreamingContext(jsc, new Duration(1000));
    	System.out.println( "2**************************!" );
		config = HBaseConfiguration.create();
		// config.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, 2181);
		config.addResource(new Path("file:///etc/hbase/conf/hbase-site.xml"));
		config.set(TableInputFormat.INPUT_TABLE, "tweetsDB");
    HBaseAdmin hBaseAdmin = null;
    	
        try  {
        	
		

			newAPIJobConfiguration = Job.getInstance(config);
			newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "tweetTB");
			newAPIJobConfiguration.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
			
        	

			System.out.println("Creating table.... ");
        	HTableDescriptor table = new HTableDescriptor(TableName.valueOf("tweetTB"));
        	
        	System.out.println("Creating table 2.... ");
        	
			table.addFamily(new HColumnDescriptor("twitter").setCompressionType(Algorithm.NONE));
			System.out.println("Creating table.... 3");
		
			hBaseAdmin = new HBaseAdmin(config);
			if (hBaseAdmin.tableExists(table.getTableName()))
			{
				System.out.print("table already created.... ");
			}else {
				hBaseAdmin.createTable(table);
			}


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
						  System.out.println(rawRecord._2);
						  String record = rawRecord._2();
						  StringTokenizer st = new StringTokenizer(record,",");
						  writeRowNewHadoopAPI(newAPIJobConfiguration.getConfiguration(), rdd);

						  System.out.println("All records ********** :"+allRecord.toString()); 
					  });
					System.out.println("All records OUTER MOST :"+allRecord.size()); 
				  }
			  });
    		

    		ssc.start();
    		ssc.awaitTermination();
    		jsc.sc().stop();   	
	    	
        } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        System.out.println( "complete spark stream!" );
    }
    
    static void writeRowNewHadoopAPI(Configuration config, JavaPairRDD<String, String> records) {
    	JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = 
    			records.mapToPair(x -> {
					Gson gson = new Gson();
					Tweet tweet = gson.fromJson(x._2, Tweet.class);
    				Put put = new Put(Bytes.toBytes("rowkey." + tweet.getId() + "." +tweet.getLang() + "." + tweet.getUser().getLocation() + "." ));
					put.addColumn(Bytes.toBytes("twitter"), Bytes.toBytes("id"), Bytes.toBytes(tweet.getId()));
					put.addColumn(Bytes.toBytes("twitter"), Bytes.toBytes("lang"), Bytes.toBytes(tweet.getLang()));
					put.addColumn(Bytes.toBytes("twitter"), Bytes.toBytes("location"), Bytes.toBytes(tweet.getUser().getLocation()));		
					
    				return new Tuple2<ImmutableBytesWritable, Put>(
						new ImmutableBytesWritable(), put);});
 		hbasePuts.saveAsNewAPIHadoopDataset(config);
    }
}
