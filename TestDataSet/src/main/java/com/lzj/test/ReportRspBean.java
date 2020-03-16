package com.lzj.test;

import java.util.List;

public class ReportRspBean {
    private String[][] datas;
    private List<String> merge_list;
    private List<String> style_list;

    public String[][] getDatas() {
        return datas;
    }

    public void setDatas(String[][] datas) {
        this.datas = datas;
    }

    public List<String> getMerge_list() {
        return merge_list;
    }

    public void setMerge_list(List<String> merge_list) {
        this.merge_list = merge_list;
    }

    public List<String> getStyle_list() {
        return style_list;
    }

    public void setStyle_list(List<String> style_list) {
        this.style_list = style_list;
    }
}
