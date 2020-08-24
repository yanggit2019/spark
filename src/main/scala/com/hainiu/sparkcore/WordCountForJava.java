package com.hainiu.sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple1;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class WordCountForJava {
    public static void main(String[] args) {
        //创建SparkConf对象
        SparkConf sparkConf = new SparkConf().setAppName("wordcountforjava").setMaster("local[*]");
        //创建Java版的sparkContext对象
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        //创建rdd,rdd里面的元素是“word world word”
        JavaRDD<String> rdd = jsc.textFile("H:\\input1");
        //通过转换操作，把一行数据变成单词
        JavaRDD<String> flatMapRdd = rdd.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterator<String> call(String line) throws Exception {
                String[] arr = line.split("\t");

                return Arrays.asList(arr).iterator();
            }
        });
        //转换 “word” -->("word",1)
        JavaRDD<Tuple2<String, Integer>> pairRdd = flatMapRdd.map(new Function<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> call(String v1) throws Exception {
                return new Tuple2<String, Integer>(v1, 1);
            }
        });

        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> groupByRdd = pairRdd.groupBy(new Function<Tuple2<String, Integer>, String>() {
            @Override
            public String call(Tuple2<String, Integer> v1) throws Exception {
                return v1._1;
            }
        });
        JavaPairRDD<String, Integer> mapValueRdd = groupByRdd.mapValues(new Function<Iterable<Tuple2<String, Integer>>, Integer>() {
            @Override
            public Integer call(Iterable<Tuple2<String, Integer>> it) throws Exception {
                int sum = 0;
                for (Tuple2<String, Integer> t : it) {
                    sum += t._2;
                }
                return sum;
            }
        });
        //collect是action算子，当执行collect，会把executor端mapValuesRdd的数据全部拉取到driver端
        //因为是把executor端数据全部拉取回来，如果driver端的内存不够，那就会造成内存溢出了
        //注意使用collect的时候，要知道executor端拉取的数据量情况。
//        List<Tuple2<String,Integer>> list = mapValueRdd.collect();
//        for (Tuple2<String,Integer> t :list){
//            System.out.println(t);
//        }
        //take是拉取回来两条 
        List<Tuple2<String,Integer>> list = mapValueRdd.take(2);
        for (Tuple2<String,Integer> t :list){
            System.out.println(t);
        }
        
    }
}
