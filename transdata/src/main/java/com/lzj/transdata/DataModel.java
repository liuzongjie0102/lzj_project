package com.lzj.transdata;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DataModel {
    private int    goodsSeq;
    private String goodsClass;
    private String goodsName;
    private String goodsType;
    private String goodsUnit;
    private int    goodsNum;
    private double goodsPrice;
    private double goodsMoney;
}
