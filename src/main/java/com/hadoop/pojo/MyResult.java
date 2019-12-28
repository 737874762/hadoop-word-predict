package com.hadoop.pojo;

import lombok.Data;

import java.util.List;

/**
 * 〈〉
 *
 * @author Chkl
 * @create 2019/12/27
 * @since 1.0.0
 */
@Data
public class MyResult {
    private Double goodCount;//好评数
    private Double badCount;//差评数
    private Double correct;//正确率
    private List<PredictResult> predictResults;
}
