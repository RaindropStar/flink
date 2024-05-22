package jmzhang.study.flink.process;

import jmzhang.study.flink.bean.WaterSensor;
import jmzhang.study.flink.functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;


public class TopNDemo {
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

        //TODO 思路一:所有数据到一起，用hashmap存，key =vc，value=count值
        sensorDS.windowAll(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                        .process(new MyTopNPAWF())
                        .print();








        env.execute();
    }

    public static class MyTopNPAWF extends ProcessAllWindowFunction<WaterSensor,String, TimeWindow>{

        @Override
        public void process(ProcessAllWindowFunction<WaterSensor, String, TimeWindow>.Context context, Iterable<WaterSensor> iterable, Collector<String> collector) throws Exception {
            //定义一个hashmao用来存，key=vc,value =count 值
            HashMap<Integer, Integer> vcCountMap = new HashMap<>();
            //1. 遍历数据，统计 各个vc出现的次数
            for(WaterSensor element : iterable){
                Integer vc = element.getVc();
                if(vcCountMap.containsKey((element.getVc()))){
                    //1.1 Key存在，不是这个key的第一条数据，直接累加
                    vcCountMap.put(vc,vcCountMap.get(vc) +1);
                }else {
                    //1.2 key不存在，初始化
                    vcCountMap.put(vc,1);
                }


            }

            //2.对count值进行排序：利用list实现排序
            ArrayList<Tuple2<Integer, Integer>> datas = new ArrayList<>();
            for (Integer vc : vcCountMap.keySet()) {
                datas.add(Tuple2.of(vc,vcCountMap.get(vc)));
            }
            //对list进行排序，根据count值，降序
            datas.sort(
                    new Comparator<Tuple2<Integer, Integer>>() {
                        @Override
                        public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
                            //降序， 后 减 前
                            return o2.f1 - o1.f1;
                        }
                    }
            );

            //3.取出 count最大的2个vc
            StringBuffer outStr = new StringBuffer();

            //遍历 排序后的list，取出前2个，避免list不够两个， 元素个数 和 2 取最小值
            outStr.append("======================");
            for (int i = 0; i < Math.min(2,datas.size()); i++) {
                Tuple2<Integer, Integer> vcCount = datas.get(i);
                outStr.append("Top"+ (i+1));
                outStr.append("\n");
                outStr.append("vc=" + vcCount.f0);
                outStr.append("\n");
                outStr.append("count=" + vcCount.f1);
                outStr.append("\n");
                outStr.append("窗口结束时间=" + DateFormatUtils.format(context.window().getEnd(),"yyyy-MM-dd HH:mm:ss.SSS"));
                outStr.append("\n");
                outStr.append("======================");

            }

            collector.collect(outStr.toString());


        }
    }
}

