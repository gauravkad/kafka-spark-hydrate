
import java.io.*;


import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructType.*;

import com.google.gson.Gson;




public class sample_flatten {

           private static String TOPICNAME = "hellokafka";
           private static String OFFSET = "latest";
           private static String BROKERID = "localhost:9092";
           
           public static void main(String[] args) throws StreamingQueryException {
                         SparkSession spark = SparkSession
                                                      .builder()
                                                      .appName("Flatten Data")
                                                      .master("local[4]")                                                                                                                                                                  
                                                   .config("spark.driver.allowMultipleContexts", "true").getOrCreate();
                                 spark.sparkContext().setLogLevel("WARN");
                         Gson gson = new Gson();
Dataset<Row> df = spark
	  .readStream()
	  .format("kafka")
	  .option("kafka.bootstrap.servers", "localhost:9092")
	  .option("subscribe", "xyz")
	  .load();
df=df.selectExpr("CAST(value AS STRING)");
Dataset<Row> df1 = df.map(x -> gson.toJson(x));
//Dataset<Row>x=spark.read().json(df);
//df=df.select(from_json(col("value").cast("string"), spark.read().option("multiline",true).json("my_sample_json_file_as_schema.json").schema()).as("data")).select("data.*");
//df.printSchema();
//StreamingQuery query= df.writeStream().format("console")	
//.start();
Dataset<Row> example= spark.read().option("multiline",true).json("/home/gsahni/Desktop/example3.json");
example.show();
example.printSchema();
Dataset<Row> result=flattenDataframe(example);
result.printSchema();
result.write().format("csv").option("path", "/home/gsahni/Desktop/hellokafka.csv").save();

}
static Dataset<Row> flattenDataframe(Dataset<Row> example){
	StructField[] fields = example.schema().fields();
	int length = fields.length;
	String[] fieldNames = new String[length];
	for(int j = 0; j < length; j++) {
		fieldNames[j] = fields[j].name();
	}
   for(int i=0;i<length;i++){
        StructField field = fields[i];
         DataType fieldtype = field.dataType();
       if (fieldtype instanceof ArrayType) {
    	   Dataset<Row> flattened = example.withColumn(fieldNames[i],functions.explode(example.col(fieldNames[i])));
    	   flattened.show();
    	   return flattenDataframe(flattened);
       }
       
       else if ( fieldtype instanceof StructType) {
    	   String[] childFieldnames = ((StructType) fieldtype).fieldNames();
    	   for(int j=0;j<childFieldnames.length;j++) {
    		   childFieldnames[j]=fieldNames[i]+"."+childFieldnames[j];
    	   }
    	   List<String> newFieldnames = new ArrayList<String>(Arrays.asList(fieldNames));
    	   newFieldnames.remove(i);    	   
    	   List<String> child = new ArrayList<String>(Arrays.asList(childFieldnames));
    	   newFieldnames.addAll(child);
    	   String[] str_array = newFieldnames.toArray(new String[0]);
    	   Column[] ret = new Column[str_array.length];
    	   for (int j = 0; j < str_array.length; j++) {
    		   ret[j] = example.col(str_array[j]).as(str_array[j].replace(".", "_"));
    	   }
    	   Dataset<Row> temp=example.select(ret);
    	   temp.show();
    	   return flattenDataframe(temp);
       }
   }
   return example;
}
}
