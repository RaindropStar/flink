package jmzhang.study.flink.split;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// TODO 分流： 奇数 偶数拆分成不同流
public class SplitByFilterDemo {
    public static void main(String[] args) throws Exception {
        // TODO 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        // TODO 2. 读取数据 socket
        DataStreamSource<String> socketDS = env.socketTextStream("192.168.31.44", 7777);

//        socketDS.shuffle().print();

//        socketDS.rebalance().print();

        socketDS.filter( value -> Integer.parseInt(value) % 2 == 0).print("偶数");

        socketDS.filter( value -> Integer.parseInt(value) % 2 == 1).print("奇数");





        // TODO 5. 执行
        env.execute();

    }
}
