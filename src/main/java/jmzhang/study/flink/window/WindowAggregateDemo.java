package jmzhang.study.flink.window;

import jmzhang.study.flink.bean.WaterSensor;
import jmzhang.study.flink.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;


public class WindowAggregateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

//        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("192.168.31.44", 7777)
//                .map(new WaterSensorMapFunction());

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("192.168.31.44", 7777)
                .map(new WaterSensorMapFunction());


        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        //基于时间的，滚动窗口，窗口长度为10s
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        //窗口函数：增量聚合 Reduce
        SingleOutputStreamOperator<String> aggregate = sensorWS.aggregate(
                new AggregateFunction<WaterSensor, Integer, String>() {
                    /**
                     * 第一个类型： 输入数据的类型
                     * 第二个类型：累加器的类型，存储的中间计算结果的类型
                     * 第三个类型：输出的类型
                     * @return
                     */
                    @Override
                    public Integer createAccumulator() {
                        //创建累加器，初始化累加器
                        System.out.println("创建累加器");
                        return 0;
                    }

                    @Override
                    public Integer add(WaterSensor waterSensor, Integer integer) {
                        //聚合逻辑
                        System.out.println("调用add方法");
                        return integer + waterSensor.getVc();
                    }

                    @Override
                    public String getResult(Integer integer) {
                        //获取最终结果，窗口触发时输出
                        System.out.println("调用getResult方法");
                        return integer.toString();
                    }

                    @Override
                    public Integer merge(Integer integer, Integer acc1) {
                        //只有会话窗口才会用到
                        System.out.println("调用merge方法");
                        return null;
                    }
                }
        );

        aggregate.print();


        env.execute();
    }
}
