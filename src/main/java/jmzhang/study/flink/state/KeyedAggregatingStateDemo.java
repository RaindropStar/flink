package jmzhang.study.flink.state;

import jmzhang.study.flink.bean.WaterSensor;
import jmzhang.study.flink.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * TODO 统计每种传感器的平均水位
 */

public class KeyedAggregatingStateDemo {
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
                            AggregatingState<Integer,Double> vcAvgAggregatingState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                vcAvgAggregatingState = getRuntimeContext()
                                        .getAggregatingState(
                                                new AggregatingStateDescriptor<Integer, Tuple2<Integer,Integer>, Double>(
                                                        "vcAvgAggregatingState",
                                                        new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                                                            @Override
                                                            public Tuple2<Integer, Integer> createAccumulator() {
                                                                return Tuple2.of(0,0);
                                                            }

                                                            @Override
                                                            public Tuple2<Integer, Integer> add(Integer integer, Tuple2<Integer, Integer> integerIntegerTuple2) {
                                                                return Tuple2.of(integerIntegerTuple2.f0 + integer,integerIntegerTuple2.f1 + 1);
                                                            }

                                                            @Override
                                                            public Double getResult(Tuple2<Integer, Integer> integerIntegerTuple2) {
                                                                return integerIntegerTuple2.f0 * 1D / integerIntegerTuple2.f1;
                                                            }

                                                            @Override
                                                            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> integerIntegerTuple2, Tuple2<Integer, Integer> acc1) {
                                                                return null;
                                                            }
                                                        },
                                                        Types.TUPLE(Types.INT, Types.INT)
                                                )
                                        );

                            }

                            @Override
                            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                                //将水位值 添加到 聚合状态中
                                vcAvgAggregatingState.add(waterSensor.getVc());
                                //从聚合状态中  获取结果
                                Double vcAvg = vcAvgAggregatingState.get();

                                collector.collect("传感器id为" + waterSensor.getId() +",平均水位值=" + vcAvg);

                            }
                        }

                )
                .print();






        env.execute();

    }
}
