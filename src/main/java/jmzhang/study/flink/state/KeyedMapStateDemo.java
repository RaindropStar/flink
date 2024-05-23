package jmzhang.study.flink.state;

import jmzhang.study.flink.bean.WaterSensor;
import jmzhang.study.flink.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * TODO 统计每种传感器每种水位值出现的次数
 */

public class KeyedMapStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("192.168.31.44", 7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element,ts) -> element.getTs() * 1000L)
                );

        sensorDS.keyBy(r -> r.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            MapState<Integer,Integer> vcCountMapState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                vcCountMapState= getRuntimeContext().getMapState(new MapStateDescriptor<Integer,Integer>("vcCountMapState",Types.INT,Types.INT));

                            }

                            @Override
                            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                                //1.判断是否存在vc对应的key
                                Integer vc = waterSensor.vc;
                                if (vcCountMapState.contains(vc)) {
                                    //1.1如果包含这个vc的key，直接对waterSensor +1
                                    Integer count = vcCountMapState.get(vc);
                                    vcCountMapState.put(vc,++count);
                                    count = count + 1;
                                }else{
                                    //1.2如果不包含这个vc的key，初始化put进去
                                    vcCountMapState.put(vc,1);
                                }
                                //2.遍历map状态，输出每一个k-v的值
                                StringBuffer outStr = new StringBuffer();
                                outStr.append("传感器id为" + waterSensor.getId() +"\n");

                                for (Map.Entry<Integer, Integer> vcCount : vcCountMapState.entries()) {
                                    outStr.append(vcCount.toString()+ "\n");
                                }
                                outStr.append("===================================\n");

                                collector.collect(outStr.toString());


//                                vcCountMapState.get()
//                                vcCountMapState.contains();
//                                vcCountMapState.put();
//                                vcCountMapState.putAll();
//                                vcCountMapState.entries();
//                                vcCountMapState.keys();
//                                vcCountMapState.values();






                            }
                        }

                )
                .print();






        env.execute();

    }
}
