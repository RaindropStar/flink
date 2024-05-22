package jmzhang.study.flink.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class MyPeriodWatermarkGenerator implements WatermarkGenerator {
    private long delayTs;
    //用来保存当前为止最大的事件时间
    private long maxTs;

    public MyPeriodWatermarkGenerator(long delayTs){
        this.delayTs = delayTs;
        this.maxTs = Long.MIN_VALUE + this.delayTs + 1;

    }
    /**
     * 每条数据来，都会调用一次，用来提取最大的时间时间，保存下来
     * @param o
     * @param l
     * @param watermarkOutput
     */

    @Override
    public void onEvent(Object o, long l, WatermarkOutput watermarkOutput) {
        maxTs = Math.max(maxTs, l);


    }

    /**
     * 周期性调用，发射watermark
     * @param watermarkOutput
     */

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
        watermarkOutput.emitWatermark(new Watermark(maxTs - delayTs -1));

    }
}
