package jmzhang.study.flink.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 从文件读，新的source架构
        FileSource<String> fliesource = FileSource
                .forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path("input/input.txt")
                )
                .build();

        env.fromSource(fliesource, WatermarkStrategy.noWatermarks(),"filesource")
                .print();
//        FileSource


        env.execute();

    }
}
