package consumer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;


public class HydrateStream {

	public static void main(String[] args) throws StreamingQueryException {
		
		SparkSession spark = SparkSession
				  .builder()
				  .master("local[4]")
				  .appName("HydrateStream")
				  .getOrCreate();
//		hi
		
		spark.sparkContext().setLogLevel("WARN");
		
		Dataset<Row> df = spark
				  .readStream()
				  .format("kafka")
				  .option("kafka.bootstrap.servers", "localhost:9092")
				  .option("subscribe", "hitest")
				  .load();
		
		df.mapPartitions(
				new MapPartitionsFunction() {
		            List<String> result = new ArrayList<String>();

					public Iterator call(Iterator input) throws Exception {

		                // int curMax=-1;
		                StringBuilder sb = new StringBuilder();
		                
		                while (input.hasNext()) {
		                	// System.out.println("data = " + input.next());
		                	sb.append(input.next() + " ; ");
		                	
		                }
		                System.out.println("data = " + sb.toString());
		                
		                return result.iterator();

		            						//return null;
					}

		           
		        }, Encoders.STRING()
				).writeStream()
		.outputMode("append")
		.format("console")
		.start();
				// (MapPartitionsFunction <Iterator<Row>,Iterator<U>>) x -> x {x}, Encoder<U>				);
		
		
		Dataset<Row> df2 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
				
		StreamingQuery query = df2.writeStream()
			.outputMode("append")
			.format("console")
			.start();
		
		query.awaitTermination();

	}

}
