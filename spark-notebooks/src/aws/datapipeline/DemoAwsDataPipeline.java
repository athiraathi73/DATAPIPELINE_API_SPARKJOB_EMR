package aws.datapipeline;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import java.util.List;
import java.util.Arrays; 


public class DemoAwsDataPipeline {
	
	public static void main(String[] args) {
		
		SparkSession sparkSession = SparkSession
				.builder()
				.appName("DemoAwsDataPipeline")
				.getOrCreate();
		
		List<Integer> data =  Arrays.asList(10, 11, 12, 13, 14, 15);
		Dataset<Integer> ds = sparkSession.createDataset(data, Encoders.INT());
		ds.repartition(1).write().format("json").save(args[0]);
		sparkSession.stop();
	}

}
