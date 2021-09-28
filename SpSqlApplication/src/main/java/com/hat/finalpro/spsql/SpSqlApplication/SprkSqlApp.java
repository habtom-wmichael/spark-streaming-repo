package com.hat.finalpro.spsql.SpSqlApplication;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Hello world!
 *
 */
public class SprkSqlApp 
{
	public static void main(String[] args) throws Exception {

		SparkConf conf = new SparkConf().setAppName("SparkSQLApplication").setMaster(
				"local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.hadoopConfiguration().set(
				"mapreduce.input.fileinputformat.input.dir.recursive", "true");
		sc.hadoopConfiguration().set(
				"spark.hive.mapred.supports.subdirectories", "true");
		sc.hadoopConfiguration()
				.set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive",
						"true");

		HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(
				sc.sc());

		sqlContext.setConf("hive.input.dir.recursive", "true");
		sqlContext.setConf("hive.mapred.supports.subdirectories", "true");
		sqlContext.setConf("hive.supports.subdirectories", "true");
		sqlContext.setConf("mapred.input.dir.recursive", "true");

		sqlContext.sql("DROP TABLE IF EXISTS tweetTB");

		sqlContext
				.sql("CREATE EXTERNAL TABLE tweetTB(id STRING, language STRING, state STRING, country STRING, followers INT)"
						+ "  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' LOCATION 'hdfs://localhost:8020/user/cloudera/1-spark-sql-input'");

		Row[] results = sqlContext.sql("FROM tweetTB SELECT *").collect();
		for (Row row : results) {
			System.out.println(row);
		}

		sc.close();
	}
}