package com.yuyi.persona;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.List;

public class CFModel {

    private final JavaPairRDD<Long, List<Tuple2<Long, Float>>> similaries;

    public CFModel(JavaPairRDD<Long, List<Tuple2<Long, Float>>> similaries) {
        this.similaries = similaries;
    }

    public JavaPairRDD<Long, List<Tuple2<Long, Float>>> getSimilaries() {
        return similaries;
    }


}
