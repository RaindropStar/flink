package jmzhang.study.flink.functions;

import jmzhang.study.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

public class WaterSensorMapFunction implements MapFunction<String, WaterSensor> {
    @Override
    public WaterSensor map(String s) throws Exception {
        String[] ss = s.split(",");
        return new WaterSensor(ss[0],Long.parseLong(ss[1]),Integer.valueOf(ss[2]));
    }
}
