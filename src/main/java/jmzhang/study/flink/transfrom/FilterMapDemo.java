package jmzhang.study.flink.transfrom;

import jmzhang.study.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FilterMapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS =  env.fromElements(
                new WaterSensor("s1",1L,1),
                new WaterSensor("s1",11L,11),
                new WaterSensor("s2",2L,2),
                new WaterSensor("s3",3L,3)

        );

        SingleOutputStreamOperator<String> flatMap = sensorDS.flatMap(new FlatMapFunction<WaterSensor, String>() {
            @Override
            public void flatMap(WaterSensor waterSensor, Collector<String> collector) throws Exception {
                if ("s1".equals(waterSensor.getId())) {
                    collector.collect(waterSensor.getVc().toString());
                } else if ("s2".equals(waterSensor.getId())) {
                    collector.collect(waterSensor.getTs().toString());
                    collector.collect(waterSensor.getVc().toString());
                }
            }
        });
        flatMap.print();



        env.execute();
    }
}
