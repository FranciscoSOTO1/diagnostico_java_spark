package test.minsait.ttaa.datio;

import static minsait.ttaa.datio.common.Common.SPARK_MODE;

import org.apache.spark.sql.SparkSession;

public interface SparkSessionTestWrapper {
	
	SparkSession spark = SparkSession
			  .builder()
	            .master(SPARK_MODE)
	            .getOrCreate();

}
