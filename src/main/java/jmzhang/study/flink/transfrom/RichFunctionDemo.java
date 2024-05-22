package jmzhang.study.flink.transfrom;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.api.java.ExecutionEnvironment.createLocalEnvironmentWithWebUI;

public class RichFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4);

        SingleOutputStreamOperator<Integer> map = source.map(new RichMapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer integer) throws Exception {
                return integer + 1;
            }
        });

        map.print();

        env. execute();

    }
}
