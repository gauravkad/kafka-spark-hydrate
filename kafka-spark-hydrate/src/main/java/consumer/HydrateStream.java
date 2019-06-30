
package consumer;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import hydrate.*;

class HydrateStream {

	public static void main(String[] args) throws Exception {
		
		SparkSession spark = SparkSession
				  .builder()
				  .master("local[4]")
				  .appName("HydrateStream")
				  .getOrCreate();

		
		spark.sparkContext().setLogLevel("WARN");
		
		Dataset<Row> df = spark
				  .readStream()
				  .format("kafka")
				  .option("kafka.bootstrap.servers", "localhost:9092")
				  .option("subscribe", "hellokafka")
				  .load();
		
		StreamingQuery query = df
				.selectExpr("CAST(value AS STRING)")
				.mapPartitions(new MapPartitionsFunction() {
					
		            List<String> result = new ArrayList<String>();

					public Iterator call(Iterator input) throws Exception {
						//if(input.hasNext())
		                //System.out.println(input.next());
						Test test = new Test(); 
		                Iterator itrResult = test.calc(input);
		                return itrResult;
					}

		        }, Encoders.STRING()
				).writeStream()
		.outputMode("append")
		.format("console")
		.start();
				// (MapPartitionsFunction <Iterator<Row>,Iterator<U>>) x -> x {x}, Encoder<U>				);
		
		
		Dataset<Row> df2 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
				
		StreamingQuery query_2 = df2.writeStream()
			.outputMode("append")
			.format("console")
			.start();
		
		query.awaitTermination();
		query_2.awaitTermination();
	}
}
