package jmzhang.study.flink.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class SinkKafkaWithKey {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

//        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("192.168.31.44", 7777)
//                .map(new WaterSensorMapFunction());

        DataStreamSource<String> sensorDS = env.socketTextStream("192.168.31.44", 7777);

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                //指定Kafka的端口
                .setBootstrapServers("192.168.31.10:9092")
                //指定序列化器，指定Topic名称、具体的序列化
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<String>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(String s, KafkaSinkContext kafkaSinkContext, Long aLong) {
                                String[] datas = s.split(",");
                                byte[] key = datas[0].getBytes(StandardCharsets.UTF_8);
                                byte[] value = s.getBytes(StandardCharsets.UTF_8);
                                return new ProducerRecord<>("ws",key,value);
                            }
                        }

                )
                //写到Kafka的一致性级别，精准一次、至少一次
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //精准一次，必须设置事物超时时间
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,10*60*1000+"")
                .setTransactionalIdPrefix("jmzhang")
                .build();

        sensorDS.sinkTo(kafkaSink);


        env.execute();


    }


}
