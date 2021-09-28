package com.f.pro.hab.SpSqlhbas.appl;

import java.io.File;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructType;

import com.f.pro.hab.SpSqlhbas.appl.util.HbaseTableUtil;
import com.f.pro.hab.SpSqlhbas.pojo.Tweet;

public class SparkSqlHbaseApp {
	public static void main(String[] args) {
		System.out.println("AparkSqlApp:*********************started. ");
//       
        SparkConf sconf = new SparkConf().setAppName("KafkaStreaming").setMaster("local[*]");
		sconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sconf.registerKryoClasses(new Class[]{org.apache.hadoop.hbase.io.ImmutableBytesWritable.class});
        JavaSparkContext jsc = new JavaSparkContext(sconf);
        
        
    	HbaseTableUtil hbaseUtil  = new HbaseTableUtil(jsc, "local[*]");

    	JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = hbaseUtil.readTableByJavaPairRDD();
    	

    	SQLContext sqlContext = new SQLContext(jsc.sc());
    	JavaRDD<Tweet> rows = hBaseRDD.map(x -> {
    		Tweet tweet = new Tweet();
    		
    		tweet.setId(Bytes.toLong(x._2.getValue(Bytes.toBytes("twitter"), Bytes.toBytes("id"))));
    		tweet.setLang(Bytes.toString(x._2.getValue(Bytes.toBytes("twitter"), Bytes.toBytes("lang"))));
    		tweet.getUser().setLocation(Bytes.toString(x._2.getValue(Bytes.toBytes("twitter"), Bytes.toBytes("location"))));
//    		
    		return tweet;
		});
    	

    	
    	DataFrame tabledata = sqlContext.createDataFrame(rows, Tweet.class);
    	tabledata.registerTempTable("tweetTB");
    	tabledata.printSchema();
    	System.out.println("Query executing:*********************started. ");
    	// Queries
    	DataFrame query1 = sqlContext.sql("SELECT * FROM tweetTB");
    	query1.show();
    	
    	DataFrame query2 = sqlContext.sql("SELECT  first(coordinates),first(createdAt), id, first(text) FROM tweetTB group by id");
    	query2.show();
    	
    	System.out.println("Query executing:*********************done. ");

    	
    	
    	jsc.stop();
    	
	}
}
