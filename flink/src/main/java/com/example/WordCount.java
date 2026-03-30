package com.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink 最简单的 WordCount Demo
 * 从内存数据源读取文本，按空格分词，统计每个单词出现的次数
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 从集合中读取数据
        DataStream<String> source = env.fromElements(
                "hello flink",
                "hello world",
                "flink is great",
                "hello flink world"
        );

        // 3. 分词 -> 转成 (word, 1) -> 按 word 分组 -> 求和
        DataStream<Tuple2<String, Integer>> result = source
                .flatMap(new Tokenizer())
                .keyBy(value -> value.f0)
                .sum(1);

        // 4. 输出结果
        result.print();

        // 5. 执行
        env.execute("WordCount Demo");
    }

    /**
     * 分词器：将每行文本按空格拆分，输出 (word, 1)
     */
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            for (String word : value.split("\\s+")) {
                if (word.length() > 0) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }
}
