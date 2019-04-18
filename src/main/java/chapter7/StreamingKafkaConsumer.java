package chapter7;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
public class StreamingKafkaConsumer {

    public static void main(String[] args) throws StreamingQueryException {

        ObjectMapper objectMapper = new ObjectMapper();

        SparkSession spark = SparkSession.builder()
                .appName("StreamingKafkaConsumer")
                .master("local")
                .getOrCreate();

        //주키퍼 구동
        // zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties
        //카프카 구동
        // kafka-server-start /usr/local/etc/kafka/server.properties

        // kafka-console-producer --broker-list localhost:9092 --topic test
        // kafka-console-consumer --bootstrap-server localhost:9092 --topic test --from-beginning


        // Kafka Consumer
        //kafka.bootstrap-servers=kafka02.iwilab.com:9092
        //kafka.topic=tromm-content-real
        //kafka.group=tromm-dev-relay-group
        Dataset<Row> messagesDf = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "test")
                .load()
                .selectExpr("CAST(value AS STRING)"); // lines.selectExpr("CAST key AS STRING", "CAST value AS STRING") For key value

//        messagesDf.show(); // <-- Can't do this when streaming!
//        Dataset<String> words = messagesDf
//                .as(Encoders.STRING())
//                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

        Dataset<String> words = messagesDf
                .as(Encoders.STRING())
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String line) throws Exception {
                        Content content = objectMapper.readValue(line, Content.class);
                        String[] tags = content.getTags();
                        List<String> tagList = Arrays.asList(tags);
                        return tagList.iterator();
                    }
                }, Encoders.STRING());

        Dataset<Row> wordCounts = words.groupBy("value").count();
    //


        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

    //
            query.awaitTermination();
    }
}
