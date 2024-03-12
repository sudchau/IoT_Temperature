package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class IotTemp {
    public static void main(String[] args) throws TimeoutException {
        // Create SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("IoTStreamingApp")
                .master("local[*]")
                .getOrCreate();

        // Define schema for the streaming data
        StructType schema = new StructType()
                .add("device_id", DataTypes.IntegerType)
                .add("temperature", DataTypes.DoubleType)
                .add("timestamp", DataTypes.TimestampType);

        // Read CSV data as a streaming DataFrame
        Dataset<Row> TempStream = spark.readStream()
                .format("csv")
                .option("header", "true")
                .schema(schema)
                .load("src/main/resources/TemperatureDatasets");

        StructType maxTempSchema = new StructType()
                .add("device_id", DataTypes.IntegerType)
                .add("max_temp", DataTypes.DoubleType);

        Dataset<Row> MaxTempStream = spark.readStream()
                .format("csv")
                .option("header", "true")
                .schema(maxTempSchema)
                .load("src/main/resources/MaxTemperatureDatasets")
                .select(col("device_id").as("device_id_max"),col("max_temp"));

        Dataset<Row> joinedds = TempStream.join(MaxTempStream,col("device_id").equalTo(col("device_id_max")),"inner")
                .drop("device_id_max")
                .filter(col("temperature").gt(col("max_temp")));

        try {
            StreamingQuery query1 = joinedds.writeStream()
                    .outputMode("append")
                    .format("console")
                    .start();

            query1.awaitTermination(1000000); // Timeout after 60 seconds
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }
}
