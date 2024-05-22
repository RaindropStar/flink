package jmzhang.study.flink.sink;

import jmzhang.study.flink.bean.WaterSensor;
import jmzhang.study.flink.functions.WaterSensorMapFunction;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SinkMySQL {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

//        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("192.168.31.44", 7777)
//                .map(new WaterSensorMapFunction());

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("192.168.31.44", 7777)
                .map(new WaterSensorMapFunction());

        SinkFunction<WaterSensor> jdbcSink = JdbcSink.sink("INSERT INTO ws VALUES (?,?,?)",
                new JdbcStatementBuilder<WaterSensor>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
                        preparedStatement.setString(1, waterSensor.getId());
                        preparedStatement.setLong(2, waterSensor.getTs());
                        preparedStatement.setInt(3, waterSensor.getVc());

                    }
                }, JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(100)
                        .withBatchIntervalMs(3000)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("")
                        .withUsername("")
                        .withPassword("")
                        .withConnectionCheckTimeoutSeconds(60)
                        .build()
        );


        sensorDS.sinkTo((Sink<WaterSensor, ?, ?, ?>) jdbcSink);


        env.execute();


    }


}
