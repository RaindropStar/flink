package jmzhang.study.flink.wc;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// TODO DataStream 实现Wordcount：读socket，无界流
public class WordCountStreamUnboundedDemo {
    public static void main(String[] args) throws Exception {
        // TODO 1. 创建执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //NOTE 测试代码，本地跑查看并行度，需要引入一个依赖
       StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        // TODO 2. 读取数据 socket
//        DataStreamSource<String> socketDS = env.socketTextStream("192.168.31.44", 7777);

        // TODO 2. 读取DataGeneratorSource产生数据

        //DataGeneratorSource 产生一行四个6位随机字母，并按空格分隔
        GeneratorFunction<Long, String> generatorFunction = index ->
                RandomStringUtils.randomAlphabetic(2) +" "+RandomStringUtils.randomAlphabetic(2) +" " +RandomStringUtils.randomAlphabetic(2) + " "+ RandomStringUtils.randomAlphabetic(2);


        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                generatorFunction,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(100),
                Types.STRING
        );



        DataStreamSource<String> socketDS = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");
        // TODO 3. 处理数据,切分，转换，分组，聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketDS.flatMap((String s, Collector<Tuple2<String, Integer>> collector) -> {
            String[] words = s.split(" ");
            for (String word : words) {
                collector.collect(Tuple2.of(word, 1));
            }
        })
//                .setParallelism(24)
                .returns(Types.TUPLE(Types.STRING,Types.INT)).keyBy(
//                (Tuple2<String, Integer> value) -> {return value.f0;}//第一种写法
                  (Tuple2<String, Integer> value) -> value.f0//第二种写法n
        ).sum(1);
        // TODO 4. 输出
        sum.print();

        // TODO 5. 执行
        env.execute();

    }
}
