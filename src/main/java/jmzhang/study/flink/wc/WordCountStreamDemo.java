package jmzhang.study.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// TODO DataStream 实现Wordcount：读文件，有界流
public class WordCountStreamDemo {
    public static void main(String[] args) throws Exception {
        // TODO 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 2.读取数据
        DataStreamSource<String> lineDS = env.readTextFile("input/input.txt");

        // TODO 3.1处理数据,切分、转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // TODO 3.1.1 按照空格切分
                String[] words = s.split(" ");
                for (String word : words) {
                    // TODO 3.1.2 转换成二元组 （word，1）
                    Tuple2<String, Integer> wordsAndOne = Tuple2.of(word, 1);
                    // TODO 3.1.3 通过采集器向下游发送数据
                    collector.collect(wordsAndOne);
                }
            }
        });
        // TODO 3.2 分组
        KeyedStream<Tuple2<String, Integer>, String> wordAndOneKS = wordAndOneDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {

                return value.f0;
            }
        });

        // TODO 3.3 聚合 
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = wordAndOneKS.sum(1);

        // TODO 4.输出数据
        sumDS.print();

        // TODO 5 .执行：类似sparkstreaming最后的ssc.start
        env.execute();





    }
}
