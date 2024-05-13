package jmzhang.study.flink.env;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class EnvDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(); //环境的一些参数
        conf.set(RestOptions.BIND_PORT,"8081");

        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment(conf);
//                .getExecutionEnvironment(); //自动识别远程的还是本地的环境，不传参
//                .createRemoteEnvironment("paimon",8081,"/xxx"); //创建远程环境
//                .createLocalEnvironment(); //创建本地环境
        //流批一体，代码api同一套，可以制定为 批，也可以指定为流
        //默认是 流
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env
//                .socketTextStream("192.168.31.44", 7777)
                .readTextFile("input/input.txt")
                .flatMap(
                        (String s, Collector<Tuple2<String, Integer>> collector) -> {
                    String[] words = s.split(" ");
                    for (String word : words) {
                        collector.collect(Tuple2.of(word, 1));
                    }
                    }
                )
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(value-> value.f0)
                .sum(1)
                .print();

        env.execute();



    }
}
