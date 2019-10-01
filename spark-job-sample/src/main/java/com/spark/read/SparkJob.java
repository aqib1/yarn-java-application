package com.spark.read;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkJob {

	private static final String URL_HDFS_FILE = "/user/root/testdata";
	private static final String MASTER_URL = "local[*]";

	private SparkConf sparkConf;
	private SparkSession sparkSession;
	private StructType schema;

	public SparkJob() {
		schema = new StructType(new StructField[] {

		});
		sparkConf = new SparkConf().setMaster(MASTER_URL).setAppName(SparkJob.class.getName());

		sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
	}

	public SparkJob readCsv() {
		Dataset<Row> df = sparkSession.read().format("csv").option("header", "true").schema(schema).load(URL_HDFS_FILE);
		Dataset<Row> updated_df = df.select(df.col("*")) .filter("srch_adults_cnt > 1")
	      .filter("srch_children_cnt > 0")
	      .filter("is_booking == 0")
	      .filter("srch_rm_cnt > 0")
	      .groupBy("hotel_continent", "hotel_country", "hotel_market")
	      .agg(df.col("srch_rm_cnt").as("searchPopularity"))
	      .orderBy(df.col("searchPopularity").desc())
	      .limit(1);
		updated_df.show();
		return this;
	}

	public static void main(String[] args) {
		new SparkJob().readCsv();
	}

}
