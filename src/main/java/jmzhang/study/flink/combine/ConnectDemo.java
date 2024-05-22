package jmzhang.study.flink.combine;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3);
        DataStreamSource<Integer> source2 = env.fromElements(11, 22, 33);
        DataStreamSource<String> source3 = env.fromElements("111", "222","333");

        ConnectedStreams<Integer, String> connect = source1.connect(source3);
        SingleOutputStreamOperator<String> map = connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer integer) throws Exception {
                return integer.toString();
            }

            @Override
            public String map2(String s) throws Exception {
                return s;
            }
        });

        map.print();


        env.execute();
    }
}
