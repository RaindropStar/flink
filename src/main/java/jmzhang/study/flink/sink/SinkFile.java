package jmzhang.study.flink.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.io.File;
import java.time.ZoneId;
import java.time.Duration;


public class SinkFile {
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
        //必须开启checkpoint,否则文件名一直是inprocessing
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        GeneratorFunction<Long, String> generatorFunction = index -> "Number: " + index;


        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                generatorFunction,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(100),
                Types.STRING
        );


        DataStreamSource<String> dataGen = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");


        // TODO 输出到文件系统
        FileSink.DefaultRowFormatBuilder<String> fileSink = FileSink
                //输出行式存储文件，指定路径，指定编码
                .<String>forRowFormat(new Path("E:/tmp"), new SimpleStringEncoder<>("UTF-8"))
                //输出文件一些配置，文件名前缀、后缀
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("jmzhang-study-")
                                .withPartSuffix(".log")
                                .build()
                )
                //文件分桶
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH"))
                //文件滚动策略
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofSeconds(10))
//                                .withRolloverInterval(Duration.seconds(10))
                                .withMaxPartSize(1024 * 1024)
                                .build()

                );

        dataGen.sinkTo(fileSink.build());


        env.execute();

    }
}
