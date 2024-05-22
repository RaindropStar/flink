package jmzhang.study.flink.aggregate;

import jmzhang.study.flink.bean.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SimpleAggregateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS =  env.fromElements(
                new WaterSensor("s1",1L,1),
                new WaterSensor("s1",11L,11),
                new WaterSensor("s2",2L,2),
                new WaterSensor("s3",3L,3)

        );
        KeyedStream<WaterSensor, String> sensorKS = sensorDS
                .keyBy(new KeySelector<WaterSensor, String>() {
                    @Override
                    public String getKey(WaterSensor waterSensor) throws Exception {
                        return waterSensor.getId();
                    }
                });

        //TODO  简单聚合算子
        // 1.keyby之后才能调用

        //传位置索引的，适用于tuple类型，POJO不行

        SingleOutputStreamOperator<WaterSensor> result = sensorKS.sum("vc");
        result.print();


        env.execute();
    }
}
