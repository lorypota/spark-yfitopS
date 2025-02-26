package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Main {
    public static void main(String[] args) {
        // Disable logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf()
                .setAppName("YfitopS Analytics")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String filePath = "src/main/resources/plays.csv";

        //1a
        SparkSession spark = SparkSession
                .builder()
                .appName("YfitopS Analytics")
                .master("local[*]")
                .getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("userId", DataTypes.IntegerType, false),
                DataTypes.createStructField("songId", DataTypes.IntegerType, false),
                DataTypes.createStructField("rating", DataTypes.IntegerType, true)  // rating is nullable
        });

        Dataset<Row> playsDF = spark.read()
                .option("header", "false")
                .option("inferSchema", "false")
                .schema(schema)
                .csv(filePath);

        //1b
        JavaRDD<String> playsRDD = sc.textFile(filePath);

        sc.stop();
    }
}