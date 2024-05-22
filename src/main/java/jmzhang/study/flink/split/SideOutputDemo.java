package jmzhang.study.flink.split;

import jmzhang.study.flink.bean.WaterSensor;
import jmzhang.study.flink.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        // TODO 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        // TODO 2. 读取数据 socket
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("192.168.31.44", 7777)
                .map(new WaterSensorMapFunction());

        /**
         * TODO 使用侧输出流 实现分流
         * 需求： watersensor的数据，分出s1和s2的数据
         *
         */

        OutputTag<WaterSensor> s1Tag = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> s2Tag = new OutputTag<>("s2", Types.POJO(WaterSensor.class));

        SingleOutputStreamOperator<WaterSensor> process = sensorDS
                .process(
                        new ProcessFunction<WaterSensor, WaterSensor>() {
                            @Override
                            public void processElement(WaterSensor waterSensor, ProcessFunction<WaterSensor, WaterSensor>.Context context, Collector<WaterSensor> collector) throws Exception {
                                String id = waterSensor.getId();
                                if ("s1".equals(id)) {
                                    //如果是s1，放在侧输出流s1中
                                    /**
                                     *
                                     * 创建outPutTag对象
                                     * 第一个参数：标签名
                                     * 第二个参数：放入侧输出流中的类型，必须是Typeinformation
                                     */

                                    /**
                                     * 上下文调用output，将数据放入到侧输出流
                                     * 第一个参数：tag对象
                                     * 第二个参数：放入侧输出流中的数据
                                     */
                                    context.output(s1Tag, waterSensor);
                                } else if ("s2".equals(id)) {
                                    //如果是s2，放在侧输出流s2中

                                    /**
                                     * 上下文调用output，将数据放入到侧输出流
                                     * 第一个参数：tag对象
                                     * 第二个参数：放入侧输出流中的数据
                                     */
                                    context.output(s2Tag, waterSensor);

                                } else {
                                    //如果非是s1、s2，放在主流中
                                    collector.collect(waterSensor);

                                }
                            }
                        }
                );
        //打印主流
        process.print("主流");

        //打印 侧输出流s1
        process.getSideOutput(s1Tag).print("s1侧输出流");

        //打印 侧输出流s1
        process.getSideOutput(s2Tag).print("s2侧输出流");




        // TODO 5. 执行
        env.execute();
    }
}
