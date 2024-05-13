package jmzhang.study.flink.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataGeneratorDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         * 数据生成器source主要有四个参数：
         * 第一个，接口，GeneratorFunction，需要实现重写main方法
         * 第二个，Long类型，自动生成的数字序列
         * 第三个，限速策略，比如每秒生成几条数据
         * 第四个，返回的类型
         */

        env.setParallelism(5);

        GeneratorFunction<Long, String> generatorFunction = index -> "Number: " + index;


        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                generatorFunction,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(10),
                Types.STRING
        );


        env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(),"data-generator")
                .print();


        env.execute();

    }
}
