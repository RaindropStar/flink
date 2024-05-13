package jmzhang.study.flink.transfrom;

import jmzhang.study.flink.bean.WaterSensor;
import jmzhang.study.flink.functions.MapFunctionImpl;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS =  env.fromElements(
                new WaterSensor("s1",1L,1),
                new WaterSensor("s1",11L,11),
                new WaterSensor("s2",2L,2),
                new WaterSensor("s3",3L,3)

        );
        SingleOutputStreamOperator<WaterSensor> filter = sensorDS.filter(new FilterFunction<WaterSensor>() {
            @Override
            public boolean filter(WaterSensor waterSensor) throws Exception {
                return "s1".equals(waterSensor.getId());
            }
        });

        filter.print();


        env.execute();
    }
}
