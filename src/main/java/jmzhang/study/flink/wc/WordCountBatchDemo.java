package jmzhang.study.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import static java.io.FileDescriptor.out;


//TODO  DataSet API 实现 wordCount，已过时（不推荐）
public class WordCountBatchDemo {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //TODO 2.读取数据,从文件中读取数据
        DataSource<String> lineDS = env.readTextFile("input/input.txt");

        //TODO 3.切分、转换（word，1）
        FlatMapOperator<String,Tuple2<String,Integer>> wordAndOne = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // TODO 3.1 按照空格切分单词
                String[] words = s.split(" ");
                // TODO 3.2 将单词转换为 （word，1）
                for(String word : words){
                    Tuple2<String,Integer> wordTuple2 = Tuple2.of(word,1);
                    //TODO 3.3 使用Collector 向下游发送数据
                    collector.collect(wordTuple2);
                }
            }
        });

        //TODO 4.按照word分组
        UnsortedGrouping<Tuple2<String,Integer>> wordAndOneGroupBy = wordAndOne.groupBy(0);

        //TODO 5.各分组内聚合
        AggregateOperator<Tuple2<String,Integer>> sum = wordAndOneGroupBy.sum(1);//1 是位置，表示第二个元素

        //TODO 6.输出
        sum.print();

    }
}
