package jmzhang.study.flink.partition;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionCustomDemo {
    public static void main(String[] args) throws Exception {

        // TODO 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        // TODO 2. 读取数据 socket
        DataStreamSource<String> socketDS = env.socketTextStream("192.168.31.44", 7777);

        socketDS
                .partitionCustom(new MyPartitioner(),r -> r)
                        .print();







        // TODO 5. 执行
        env.execute();

    }
}
