package org.example;

import scala.Tuple2;

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

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class Main {
    public static void main(String[] args) {
        // Disable logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        String filePath = "src/main/resources/plays.csv";

        SparkSession spark = SparkSession
                .builder()
                .appName("YfitopS Analytics")
                .master("local[*]")
                .getOrCreate();

        // Q1a
        StructType schema = DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("user_id", DataTypes.StringType, true),
            DataTypes.createStructField("play_id", DataTypes.StringType, true),
            DataTypes.createStructField("rating", DataTypes.DoubleType, true)
        });

        Dataset<Row> data = spark.read()
            .option("header", "false")
            .schema(schema)
            .csv(filePath);

        // Q1b
        JavaRDD<String> lines = spark.read().textFile(filePath).javaRDD();

        // Q2
        // Only keep the rows with at least 3 columns (so with a rating)
        // Then map the rows to a tuple with the user id and a tuple with the count and the rating
        JavaPairRDD<String, Tuple2<Integer,Double>> userRatings = lines
                .map(line -> line.split(","))
                .filter(parts -> parts.length >= 3)
                .mapToPair(parts -> {
                String userId = parts[0];
                double rating = Double.parseDouble(parts[2]);
                return new Tuple2<>(userId, new Tuple2<>(1, rating));
                });

        // Add the counts and ratings for each user. Remove the users with less than 10 ratings
        JavaPairRDD<String, Tuple2<Integer,Double>> userRatingsCount = userRatings
                .reduceByKey((a, b) -> new Tuple2<>(a._1() + b._1(), a._2() + b._2()))
                .filter(tuple -> tuple._2()._1() >= 10);

        // Calculate the average rating for each user
        JavaPairRDD<String, Double> userAverageRatings = userRatingsCount
                .mapValues(tuple -> tuple._2() / tuple._1());

        // Find the user with the highest average rating
        Tuple2<String, Double> topUser = userAverageRatings
                .reduce((a, b) -> a._2() > b._2() ? a : b);

        System.out.println("Top user: " + topUser._1() + " with average rating: " + topUser._2());

        spark.stop();
    }
}