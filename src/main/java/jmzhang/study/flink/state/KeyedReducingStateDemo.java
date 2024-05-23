package jmzhang.study.flink.state;

import jmzhang.study.flink.bean.WaterSensor;
import jmzhang.study.flink.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Map;

/**
 * TODO 统计每种传感器的水位和
 */

public class KeyedReducingStateDemo {
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
                            ReducingState<Integer> vcSumReducingState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                vcSumReducingState=getRuntimeContext()
                                        .getReducingState(
                                                new ReducingStateDescriptor<Integer>(
                                                        "vcSumReducingState",
                                                        new ReduceFunction<Integer>() {
                                                            @Override
                                                            public Integer reduce(Integer integer, Integer t1) throws Exception {
                                                                return integer + t1;
                                                            }
                                                        },
                                                        Types.INT
                                                )

                                        );

                            }

                            @Override
                            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                                //来一条数据，添加到reducing状态里
                                vcSumReducingState.add(waterSensor.getVc());
                                Integer vcSum = vcSumReducingState.get();
                                collector.collect("传感器Id为" + waterSensor.getId() +",水位值总和" + vcSum );


//                                vcSumReducingState.get(); //对本组的reducing状态，获取结果
//                                vcSumReducingState.add(); //对本组的reducing状态，添加数据
//                                vcSumReducingState.clear(); //对本组的reducing状态，清空数据



                            }
                        }
                )
                .print();






        env.execute();

    }
}
