package jmzhang.study.flink.watermark;

import jmzhang.study.flink.bean.WaterSensor;
import jmzhang.study.flink.functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;


public class WaterMarkAllowLatnessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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

        OutputTag<WaterSensor> lateTag = new OutputTag<>("late-data", Types.POJO(WaterSensor.class));

        SingleOutputStreamOperator<String> process = sensorDSWithWaterMark
                .keyBy(sensor -> sensor.getId())
                //使用 事件时间语义的窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(2)) //允许推迟2s关窗
                .sideOutputLateData(lateTag) //关窗后的迟到数据，放入侧输出流
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


                );

        process.print();
        //从主流获取测流，然后打印
        process.getSideOutput(lateTag).printToErr();


        env.execute();
    }
}
/**
 * 乱序： 数据的顺序乱了，出现时间小的比时间大的晚来
 * 迟到： 数据的时间戳 小于当前 watermark
 *
 * 2. 乱序、迟到数据的处理
 * 1） watermark中乱序等待时间
 * 2） 如果开窗，设置窗口允许迟到
 * 3） 关窗后的迟到数据，放入侧输出流
 *  TODO 窗口允许迟到
 *  =》 推迟关窗时间，在关窗之前，迟到的数据来了，还能被窗口计算，来一条迟到数据触发计算一次
 *  =》 关窗后，迟到数据不被计算
 *
 *
 *  如果watermark等待3s，窗口允许迟到2s，为什么不直接watermark等待5s 或者允许窗口迟到5s
 *  =》 watermark 等待时间不会设太大 ===》 影响计算延迟
 *   如果3s ==》 窗口第一次触发计算和输出，13s的数据来  13-3=10s
 *   如果5s ==》 窗口第一次触发计算和输出，15s的数据来  15-5=10s
 *
 *   窗口允许迟到，是对 大部分迟到的数据的处理，尽量让结果准确
 *    如果只设置 允许迟到5s，那么会导致 频繁  重新输出
 *
 *    TODO 设置经验
 *    1.watermark等待时间，设置一个不算特别大，一般是秒级，在乱序 和延迟取舍
 *    2.设置一定的窗口允许迟到，只考虑大部分迟到的数据，极端小部分迟到很久的数据，不管
 *    3.极端小部分迟到很久的数据，放入测输出流
 */
