package com.feiniu.ml;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-22 09:04
 */
public final class MathFunction {
    public static double sigmoid(double v){  
        return 1.0/(1.0+Math.exp(v));
    }
}
