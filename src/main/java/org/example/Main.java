package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.List;

public class Main {
    public static void main(String[] args) {
        // Disable logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf()
                .setAppName("yfitopS Analytics")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String filePath = "src/main/resources/plays.csv";

        //1a
        SparkSession spark = SparkSession
                .builder()
                .appName("yfitopS Analytics")
                .master("local[*]")
                .getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("userId", DataTypes.IntegerType, false),
                DataTypes.createStructField("songId", DataTypes.IntegerType, false),
                DataTypes.createStructField("rating", DataTypes.IntegerType, true)
        });

        Dataset<Row> playsDF = spark.read()
                .option("header", "false")
                .option("inferSchema", "false")
                .schema(schema)
                .csv(filePath);

        //1b
        JavaRDD<String> playsRDD = sc.textFile(filePath);

        //2
        JavaRDD<Integer[]> userWithRatingRDD = playsRDD
                .filter(line -> {
                    String[] parts = line.split(",");
                    return parts.length == 3;
                })
                .map( line -> {
                    String[] parts = line.split(",");
                    return new Integer[] {Integer.parseInt(parts[0]), Integer.parseInt(parts[2])};
                });

        JavaPairRDD<Integer, Tuple2<Integer, Integer>> userWithRatingCountRDD = userWithRatingRDD
                .mapToPair(arr -> new Tuple2<>(arr[0], new Tuple2<>(arr[1], 1)))
                .reduceByKey((tuple1, tuple2) ->
                    new Tuple2<>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2)
                );

        JavaPairRDD<Integer, Tuple2<Integer, Integer>> tenRatingsOrMoreRDD = userWithRatingCountRDD
                .filter(tuple -> tuple._2._2>=10);

        JavaPairRDD<Integer, Double> avgRatingRDD = tenRatingsOrMoreRDD
                .mapValues(tuple -> (double) tuple._1/tuple._2);

        Tuple2<Integer, Double> topUser = avgRatingRDD
                .reduce((tuple1, tuple2) -> tuple1._2 > tuple2._2 ? tuple1 : tuple2);

        System.out.println(topUser);

        sc.stop();
    }
}