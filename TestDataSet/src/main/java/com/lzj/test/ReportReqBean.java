package com.lzj.test;

import java.util.List;

public class ReportReqBean {
    private int            query_type;//查询方式1-明细 2-聚合
//    private AfQueryBean     afQueryBean;//网管请求参数
    private List<RorCInfo> colDimList;//列字段
    private List<RorCInfo> rowDimList;//行字段
    private RorCInfo       subtotalField;//小计字段
    private boolean        needTitle=true;//是否需要标题
    private boolean        needTotal = false;//是否需要总计
    private String[][]     datas;//原始数据集
    private List<RorCInfo> hiddenDimList;//隐藏维度

    public int getQuery_type() {
        return query_type;
    }

    public void setQuery_type(int query_type) {
        this.query_type = query_type;
    }

//    public AfQueryBean getAfQueryBean() {
//        return afQueryBean;
//    }

//    public void setAfQueryBean(AfQueryBean afQueryBean) {
//        this.afQueryBean = afQueryBean;
//    }

    public List<RorCInfo> getColDimList() {
        return colDimList;
    }

    public void setColDimList(List<RorCInfo> colDimList) {
        this.colDimList = colDimList;
    }

    public List<RorCInfo> getRowDimList() {
        return rowDimList;
    }

    public void setRowDimList(List<RorCInfo> rowDimList) {
        this.rowDimList = rowDimList;
    }

    public RorCInfo getSubtotalField() {
        return subtotalField;
    }

    public void setSubtotalField(RorCInfo subtotalField) {
        this.subtotalField = subtotalField;
    }

    public boolean isNeedTitle() {
        return needTitle;
    }

    public void setNeedTitle(boolean needTitle) {
        this.needTitle = needTitle;
    }

    public boolean isNeedTotal() {
        return needTotal;
    }

    public void setNeedTotal(boolean needTotal) {
        this.needTotal = needTotal;
    }

    public String[][] getDatas() {
        return datas;
    }

    public void setDatas(String[][] datas) {
        this.datas = datas;
    }

    public List<RorCInfo> getHiddenDimList() {
        return hiddenDimList;
    }

    public void setHiddenDimList(List<RorCInfo> hiddenDimList) {
        this.hiddenDimList = hiddenDimList;
    }
}
