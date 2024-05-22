
package jmzhang.study.flink.window;

import jmzhang.study.flink.bean.WaterSensor;
import jmzhang.study.flink.functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class TimeWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

//        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("192.168.31.44", 7777)
//                .map(new WaterSensorMapFunction());

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("192.168.31.44", 7777)
                .map(new WaterSensorMapFunction());


        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

//        //基于时间的，滚动窗口，窗口长度为10s
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        //基于时间的，滑动窗口，窗口长度为10s，步长5s
//        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)));

        //基于时间的，会话窗口，间隔5s
//        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));
        //会话窗口，动态间隔
//        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(ProcessingTimeSessionWindows.withDynamicGap(
//                new SessionWindowTimeGapExtractor<WaterSensor>() {
//                    @Override
//                    public long extract(WaterSensor waterSensor) {
//                        return waterSensor.getTs() * 1000L;
//                    }
//                }
//        ));

        //窗口函数：增量聚合 Reduce
        SingleOutputStreamOperator<String> process = sensorWS.process(
                new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> iterable, Collector<String> collector) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        String windowsStart = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowsEnd = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");

                        long count = iterable.spliterator().estimateSize();

                        collector.collect("key=" + s + "的窗口[" + windowsStart + "," + windowsEnd + ")包含" + count + "条数据 ===>" + iterable.toString());


                    }
                }
        );

        process.print();


        env.execute();
        /**
         *
         * 触发器、移除器： 现成的几个窗口都有默认的实现，一般不需要自定义
         * TODO 1,窗口什么时候触发 输出
         * 时间进展 >= 窗口最大的时间戳（end -1ms）
         *
         * TODO 2 窗口是怎么划分的
         * start= 向下取整，取窗口长度的整数倍
         * end= start+ 窗口长度
         * 窗口左闭右开 --> 属于本窗口的最大时间戳 =end -1ms
         *
         * TODO 3 窗口的生命周期？
         * 创建：属于本窗口第一条数据来的时候，现 new 的，放入一个singletong的单例集合中
         * 销毁（关窗）： 时间进展  >= 窗口最大的时间戳 -1ms + 允许迟到的时间（默认是0 ）
         *
         *
         * trigger  触发器
         *
         * evictor 移除器
         *
         *
         */

    }
}
