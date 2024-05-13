package jmzhang.study.flink.functions;

import jmzhang.study.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

public class MapFunctionImpl implements MapFunction<WaterSensor,String> {
    @Override
    public String map(WaterSensor waterSensor) throws Exception {
        return waterSensor.getId();
    }
}
