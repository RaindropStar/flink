package jmzhang.study.flink.combine;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class ConnectKeyByDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        DataStreamSource<Tuple2<Integer, String>> source1 = env.fromElements(
                Tuple2.of(1, "a1"),
                Tuple2.of(1, "a2"),
                Tuple2.of(2, "b"),
                Tuple2.of(3, "c")
        );

        DataStreamSource<Tuple3<Integer, String,Integer>> source2 = env.fromElements(
                Tuple3.of(1, "a11",1),
                Tuple3.of(1, "a22",2),
                Tuple3.of(2, "bb",1),
                Tuple3.of(3, "cc",1)
        );


        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connect = source1.connect(source2);

        //多并行度下，需要根据 关联条件 keyby，才能保证key相同到一起去，才能匹配上

        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connectKeyBy = connect.keyBy(s1 -> s1.f0, s2 -> s2.f0);
        /**
         * 实现互相匹配的效果
         * 1. 两条流，不一定谁的数据先来
         * 2. 两条流，有数据来，存在一个变量中
         * 存在hashmap中， key =id ,value = lists数据
         * 3. 每条流有数据来的时候，除了存在变量中，不知道对方是否有匹配的数据，要去另一条流存的变量中 查找是否有匹配上的
         */


        SingleOutputStreamOperator<String> process = connectKeyBy.process(
                new CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>() {
                    HashMap<Integer, List<Tuple2<Integer, String>>> s1Cache = new HashMap<>();
                    HashMap<Integer, List<Tuple3<Integer, String, Integer>>> s2Cache = new HashMap<>();

                    @Override
                    public void processElement1(Tuple2<Integer, String> integerStringTuple2, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context context, Collector<String> collector) throws Exception {
                        //TODO 1. s1的数据来了，存在变量中
                        Integer id = integerStringTuple2.f0;
                        if (!s1Cache.containsKey(id)) {
                            //1.1 如果key不存在，说明是key的第一条，初始化
                            ArrayList<Tuple2<Integer, String>> s1Values = new ArrayList<>();
                            s1Values.add(integerStringTuple2);
                            s1Cache.put(id, s1Values);
                        } else {
                            //1.2 key存在，不是该key的第一条数据，直接添加到value的list中
                            s1Cache.get(id).add(integerStringTuple2);
                        }


                        //TODO 2. 去s2 Cache中查找是否有id,匹配上就输出，没有就不输出
                        if (s2Cache.containsKey(id)) {
//                            List<Tuple3<Integer, String, Integer>> s2Values = s2Cache.get(id);
                            for (Tuple3<Integer, String, Integer> s2Element : s2Cache.get(id)) {
                                collector.collect("s1:" + integerStringTuple2 + "<=========>" + "s2:" + s2Element);

                            }

                        }


                    }

                    @Override
                    public void processElement2(Tuple3<Integer, String, Integer> integerStringIntegerTuple3, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context context, Collector<String> collector) throws Exception {
                        //TODO 1. s2的数据来了，存在变量中
                        Integer id = integerStringIntegerTuple3.f0;
                        if (!s2Cache.containsKey(id)) {
                            //1.1 如果key不存在，说明是key的第一条，初始化
                            ArrayList<Tuple3<Integer, String, Integer>> s2Values = new ArrayList<>();
                            s2Values.add(integerStringIntegerTuple3);
                            s2Cache.put(id, s2Values);
                        } else {
                            //1.2 key存在，不是该key的第一条数据，直接添加到value的list中
                            s2Cache.get(id).add(integerStringIntegerTuple3);
                        }


                        //TODO 2. 去s1 Cache中查找是否有id,匹配上就输出，没有就不输出
                        if (s1Cache.containsKey(id)) {
//                            List<Tuple3<Integer, String, Integer>> s2Values = s2Cache.get(id);
                            for (Tuple2<Integer, String> s1Element : s1Cache.get(id)) {
                                collector.collect("s1:" + s1Element + "<=========>" + "s2:" + integerStringIntegerTuple3);

                            }

                        }

                    }
                }
        );

        process.print();


        env. execute();

    }
}
