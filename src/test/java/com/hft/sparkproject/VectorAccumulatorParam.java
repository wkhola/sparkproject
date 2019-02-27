package com.hft.sparkproject;

import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Vector;

/**
 * @author : kai.wu
 * @date : 2019/2/27
 */
class VectorAccumulatorParam implements AccumulatorParam<Vector> {
    private static final long serialVersionUID = -233611816402659506L;

    @Override
    //合并两个累加器的值。
    //参数r1是一个累加数据集合
    //参数r2是另一个累加数据集合
    public Vector addInPlace(Vector r1, Vector r2) {
        r1.addElement(r2);
        return r1;
    }
    @Override
    //初始值
    public Vector zero(Vector initialValue) {
        return initialValue;
    }
    @Override
    //添加额外的数据到累加值中
    //参数t1是当前累加器的值
    //参数t2是被添加到累加器的值
    public Vector addAccumulator(Vector t1, Vector t2) {
        t1.addElement(t2);
        System.out.println(t1);
        return t1;
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("test");

        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 2);
        JavaRDD<Integer> javaRDD = sc.parallelize(data,5);

        Accumulator<Integer> accumulator = sc.accumulator(0);
        Vector<Integer> initialValue = new Vector<>();
        for(int i=6;i<9;i++){
            initialValue.add(i);
        }

        //自定义累加器
        Accumulator accumulator1 = sc.accumulator(initialValue, new VectorAccumulatorParam());
        JavaRDD<Integer> result = javaRDD.map(
                (Function<Integer, Integer>) v1 -> {
                    accumulator.add(v1);
                    Vector<Integer> term = new Vector<>();
                    term.add(v1);
                    System.out.println(term);
                    accumulator1.add(term);
                    return v1;
                });
        System.out.println(result.collect());
        System.out.println("~~~~~~~~~~~~~~~~~~~~~" + accumulator.value());
        System.out.println("~~~~~~~~~~~~~~~~~~~~~" + accumulator1.value());

    }

}

