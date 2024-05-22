package jmzhang.study.flink.window;

import jmzhang.study.flink.bean.WaterSensor;
import jmzhang.study.flink.functions.WaterSensorMapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;


public class WindowApiDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

//        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("192.168.31.44", 7777)
//                .map(new WaterSensorMapFunction());

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("192.168.31.44", 7777)
                .map(new WaterSensorMapFunction());


        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        //TODO 1. 指定窗口的分配器：指定用哪一种窗口  --- 时间 or 计数？ 滚动、滑动、会话？
        // 1.1 没有keyby的窗口： 窗口内的所有数据进入同一个子任务，并行度只能为1

//        sensorDS.windowAll()
        // 1.2 有keyby的窗口： 每个key上都定义一组窗口，各自独立进行统计计算

        //基于时间的，滚动窗口，窗口长度为10s
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        //基于时间的，滑动窗口，窗口长度为10s，滑动步长2s
//        sensorKS.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2)));
        //基于时间的，会话窗口，超时间间隔5s
//        sensorKS.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
//        //基于计数的，滚动窗口，窗口长度-5个元素
//        sensorKS.countWindow(5);

//        //基于计数的，滑动窗口，窗口长度-5个元素，滑动步长=2个元素
//        sensorKS.countWindow(5,2);
        //全局窗口，计数窗口的底层用的就是这个
//        sensorKS.window(GlobalWindows.create());

        //TODO 2. 指定窗口函数
        //增量聚合：来一条数据，计算一条数据，窗口触发的时候输出计算结果
//        sensorWS.reduce();
        //全窗口函数：数据来了不计算，存起来，窗口触发的时候，计算并输出结果



        env.execute();
    }
}
