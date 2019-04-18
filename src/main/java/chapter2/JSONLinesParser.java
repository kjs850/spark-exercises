package chapter2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JSONLinesParser {

    public void parseJsonLines(){
        SparkSession spark = SparkSession.builder()
                .appName("Json Line to Dataframe")
                .master("local")
                .getOrCreate();

//        Dataset<Row> df =  spark.read().format("json")
//                .load("src/main/resources/chapter2/simple.json");

        Dataset<Row> df2 =  spark.read().format("json")
                .option("multiline", true)
                .load("src/main/resources/chapter2/multiline.json");

        df2.show(5, 150);
        df2.printSchema();

    }
}
