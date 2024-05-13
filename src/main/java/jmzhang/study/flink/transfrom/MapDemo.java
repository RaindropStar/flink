package jmzhang.study.flink.transfrom;

import jmzhang.study.flink.bean.WaterSensor;
import jmzhang.study.flink.functions.MapFunctionImpl;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.stream.Stream;

public class MapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS =  env.fromElements(
                new WaterSensor("s1",1L,1),
                new WaterSensor("s2",2L,2),
                new WaterSensor("s3",3L,3)

        );
        //map算子，一进一出
//        //TODO 方式一：匿名实现类
//        SingleOutputStreamOperator<String> map = sensorDS.map(new MapFunction<WaterSensor, String>() {
//            @Override
//            public String map(WaterSensor waterSensor) throws Exception {
//                return waterSensor.getId();
//            }
//        });

        //TODO 方式二：lambda表达式
//        SingleOutputStreamOperator<String> map = sensorDS.map(sensor -> sensor.getId());

        //TODO 方式三：定义一个类来实现MapFunction
//        SingleOutputStreamOperator<String> map = sensorDS.map(new MyMapFunction());
        SingleOutputStreamOperator<String> map = sensorDS.map(new MapFunctionImpl());

        map.print();






        env.execute();

    }

    public static class MyMapFunction implements MapFunction<WaterSensor,String>{
        @Override
        public String map(WaterSensor waterSensor) throws Exception {
            return waterSensor.getId();
        }
    }
}
