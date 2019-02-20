package com.hft.sparkproject.spark;

import org.apache.spark.AccumulatorParam;

/**
 * @author : kai.wu
 * @date : 2019/2/20
 */
public class SessionAggrStatAccumulator implements AccumulatorParam<String> {

    private static final long serialVersionUID = -7345165348233274962L;

    @Override
    public String addAccumulator(String t1, String t2) {
        return null;
    }

    @Override
    public String addInPlace(String r1, String r2) {
        return null;
    }

    @Override
    public String zero(String initialValue) {


        return null;
    }
}
