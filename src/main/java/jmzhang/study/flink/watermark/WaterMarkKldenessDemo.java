package jmzhang.study.flink.watermark;

import jmzhang.study.flink.bean.WaterSensor;
import jmzhang.study.flink.functions.WaterSensorMapFunction;
import jmzhang.study.flink.partition.MyPartitioner;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;


public class WaterMarkKldenessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        /**
         * 自定义分析器，数据 % 分区数，只输入奇数，都会去往map的一个子任务
         */
        SingleOutputStreamOperator<Integer> socketDS = env.socketTextStream("192.168.31.44", 7777)
                .partitionCustom(new MyPartitioner(), r -> r)
                .map(r -> Integer.parseInt(r))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Integer>forMonotonousTimestamps()
                                .withTimestampAssigner((r, ts) -> r * 1000L)
                                .withIdleness(Duration.ofSeconds(5)) //空闲等待5s
                );

        //分成两组：奇数一组，偶数一组，开10s事件时间滚动窗口

        socketDS
                .keyBy(r -> r % 2)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(
                        new ProcessWindowFunction<Integer, String, Integer, TimeWindow>() {
                            @Override
                            public void process(Integer integer, ProcessWindowFunction<Integer, String, Integer, TimeWindow>.Context context, Iterable<Integer> iterable, Collector<String> collector) throws Exception {
                                long start = context.window().getStart();
                                long end = context.window().getEnd();
                                String windowsStart = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
                                String windowsEnd = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");

                                long count = iterable.spliterator().estimateSize();

                                collector.collect("key=" + integer + "的窗口[" + windowsStart + "," + windowsEnd + ")包含" + count + "条数据 ===>" + iterable.toString());

                            }
                        }
                ).print();




        env.execute();
    }
}
/**
 *  TODO 内置watermark的生成原理
 *  1. 都是周期性生成的： 默认200毫秒
 *  2. 有序流： watermark = 当前最大的时间事件 -ms
 *  3. 乱序流： watermark = 当前最大的时间事件- 延迟时间 -ms
 */
