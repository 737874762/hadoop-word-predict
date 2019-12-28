package com.hadoop.pojo;

import lombok.Data;

/**
 * 〈预测结果展现对象〉
 *
 * @author Chkl
 * @create 2019/12/24
 * @since 1.0.0
 */

@Data
public class PredictResult {
    private String lineNum;//行号
    private String pResult;//预测结果
    private String tResult;//实际结果
}
