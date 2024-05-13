package jmzhang.study.flink.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 从kafka读，新的source架构
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("flink01:9092")  //制定kafka节点的地址和端口
                .setGroupId("jmzhang")  //指定消费者组id
                .setTopics("topic_1")   //指定消费的Topic
                .setValueOnlyDeserializer(new SimpleStringSchema()) //指定反序列化器，这个是反序列化value
                .setStartingOffsets(OffsetsInitializer.latest()) //flink消费kafka的策略
                .build();


        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),"kafkasource")
                .print();

        env.execute();
    }
}
