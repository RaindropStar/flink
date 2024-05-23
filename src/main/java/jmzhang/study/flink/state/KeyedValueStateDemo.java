package jmzhang.study.flink.state;

import jmzhang.study.flink.bean.WaterSensor;
import jmzhang.study.flink.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * TODO 检测每种传感器的水位值，如果连续的两个水位值超过10，就输出报警
 */

public class KeyedValueStateDemo {
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
                            // TODO 1,定义状态
                            ValueState<Integer> lastVcState;


                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                //TODO 2,在open方法中，初始化状态
                                //状态描述器的两个参数，第一个参数，起个名字，唯一不重复，第二个参数，存储的类型
                                lastVcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVcState", Types.INT));

                            }

                            @Override
                            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
//                                lastVcState.value(); //取出值状态里的数据
//                                lastVcState.update();  //更新值状态里的数据
//                                lastVcState.clear();  //清除 值状态里的数据
                                //1.取出上一条数据的水位值(Integer 默认值是null，判断)
                                int lastVc = lastVcState.value() == null ? 0 : lastVcState.value();
                                Integer vc = waterSensor.getVc();

                                //2.求差值绝对值，判断是否超过10
                                if (Math.abs(vc - lastVc) > 10) {
                                    collector.collect("传感器="+waterSensor.getId()+"==>当前水位值=" +vc+",与上一条水位值="+lastVc+",相差超过10！！");
                                }

                                //3.更新状态里自己的水位值
                                lastVcState.update(vc);
                            }
                        }
                )
                .print();






        env.execute();

    }
}
