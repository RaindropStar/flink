package jmzhang.study.flink.watermark;

import jmzhang.study.flink.bean.WaterSensor;
import jmzhang.study.flink.functions.WaterSensorMapFunction;
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


public class WaterMarkOutOfOrdernessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        /**
         * 1. 如果接收到上游多个，取最小
         * 2.往下游发送，广播
         */
//        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

//        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("192.168.31.44", 7777)
//                .map(new WaterSensorMapFunction());

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("192.168.31.44", 7777)
                .map(new WaterSensorMapFunction());

        // TODO 指定 watermark策略
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
//                //升序的watermark
//                .<WaterSensor>forMonotonousTimestamps()
                //指定watermark生成，乱序的，等待3s
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                //指定时间戳分配器，从数据中提取
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor waterSensor, long l) {
                        //返回的时间戳，要 毫秒
                        System.out.println("数据:" + waterSensor + ",recordTs" + l);
                        return waterSensor.getTs() * 1000L;
                    }
                });

        SingleOutputStreamOperator<WaterSensor> sensorDSWithWaterMark = sensorDS.assignTimestampsAndWatermarks(watermarkStrategy);

        sensorDSWithWaterMark
                .keyBy(sensor -> sensor.getId())
                //使用 事件时间语义的窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(
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


                )
                .print();



        env.execute();
    }
}
/**
 *  TODO 内置watermark的生成原理
 *  1. 都是周期性生成的： 默认200毫秒
 *  2. 有序流： watermark = 当前最大的时间事件 -ms
 *  3. 乱序流： watermark = 当前最大的时间事件- 延迟时间 -ms
 */
