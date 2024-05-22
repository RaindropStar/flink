package jmzhang.study.flink.process;

import jmzhang.study.flink.bean.WaterSensor;
import jmzhang.study.flink.functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;


public class KeydProcessTimerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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

        KeyedStream<WaterSensor, String> sensorKS = sensorDSWithWaterMark.keyBy(sensor -> sensor.getId());

        //TODO Process:Keyby
        SingleOutputStreamOperator<String> process = sensorKS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    /**
                     * 1.来一条数据调用一次
                     * @param waterSensor
                     * @param context
                     * @param collector
                     * @throws Exception
                     */
                    @Override
                    public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
                        //数据中提取出来的事件时间
                        Long ts = context.timestamp();

                        //定时器
                        TimerService timerService = context.timerService();


                        //1.事件时间的案例
//                        timerService.registerEventTimeTimer(5000L);
//                        System.out.println("当前时间是" + ts + ",注册了一个5s的定时器");
                        //2.处理时间的案例
//                        long currentTs = timerService.currentProcessingTime();
//                        timerService.registerEventTimeTimer(currentTs + 5000L);
//                        System.out.println("当前时间是" + ts + ",注册了一个5s的定时器");
                        //3.获取process当前的watermark
                        long currentWatermark = timerService.currentWatermark();
                        System.out.println("当前数据=" + currentWatermark + ",当前watermark");

//                        timerService.deleteEventTimeTimer();
//                        //获取当前处理时间
//                        long currentTs = timerService.currentProcessingTime();
//                        //获取当前的watermark
//                        long wm = timerService.currentWatermark();

                    }

                    /**
                     * 2. 时间进展到定时器注册时间，调用该方法
                     * @param timestamp  当前时间进展
                     * @param ctx        上下文
                     * @param out        采集器
                     * @throws Exception
                     */
                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        String currentKey = ctx.getCurrentKey();
                        System.out.println("key是"+ currentKey +"现在时间是" + timestamp + "定时器触发");
                    }
                }
        );

        process.print();


        env.execute();
    }
}
/**
 *  TODO 定时器
 *  1. keyed才有
 *  2.事件时间定时器，通过watermark来触发
 *    watermark >= 注册时间
 *    注意: watermark = 当前最大事件时间 -等待时间 -1ms ，因为 -1ms所以会推迟一条数据
 *    如果等待3s ，watermark= 8s - 3s -1ms = 4999ms,不会触发5s的定时器
 *   3.在process中获取当前的watermark，显示的是上衣擦 的watermark
 *   (因为process还没接收这条数据对应的watermark)
 */
