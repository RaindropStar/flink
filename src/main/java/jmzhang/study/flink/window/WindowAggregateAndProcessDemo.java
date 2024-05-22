package jmzhang.study.flink.window;

import jmzhang.study.flink.bean.WaterSensor;
import jmzhang.study.flink.functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class WindowAggregateAndProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

//        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("192.168.31.44", 7777)
//                .map(new WaterSensorMapFunction());

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("192.168.31.44", 7777)
                .map(new WaterSensorMapFunction());


        // TODO 结合两者优点
        // 1.增量聚合：来一条计算一条，存储中间的计算结果
        // 2.全窗口函数：可以通过 上下文灵活实现功能


        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        //基于时间的，滚动窗口，窗口长度为10s
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        //窗口函数：增量聚合 Reduce
        SingleOutputStreamOperator<String> aggregate = sensorWS.aggregate(
                new MyAgg(),new MyProcess()
        );

        aggregate.print();


        env.execute();
    }

    public static class MyAgg implements AggregateFunction<WaterSensor,Integer,String>{
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

    public static class MyProcess extends ProcessWindowFunction<String,String,String,TimeWindow>{
        @Override
        public void process(String s, ProcessWindowFunction<String, String, String, TimeWindow>.Context context, Iterable<String> iterable, Collector<String> collector) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            String windowsStart = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
            String windowsEnd = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");

            long count = iterable.spliterator().estimateSize();

            collector.collect("key=" + s + "的窗口[" + windowsStart + "," + windowsEnd + ")包含" + count + "条数据 ===>" + iterable.toString());


        }
    }
}
