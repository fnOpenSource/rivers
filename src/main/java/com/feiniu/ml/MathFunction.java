package com.feiniu.ml;

public final class MathFunction {
    public static double sigmoid(double v){  
        return 1.0/(1.0+Math.exp(v));
    }
}
