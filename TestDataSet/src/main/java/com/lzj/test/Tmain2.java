package com.lzj.test;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class Tmain2 {

    public static void main(String[] args) throws Exception {

        int ini_col_size = 5;//数据-列的初始化
        int ini_row_size = 6;//数据-行的初始化
//
//        String[][] datas = new String[ini_row_size][ini_col_size];
//        for (int i=0; i < ini_row_size; i++){
//            for (int j=0; j < ini_col_size; j++){
//                datas[i][j] = i + "" + j;
//            }
//        }
//
//        List<RorCInfo> col_dim_list = new ArrayList<>();
//
//        RorCInfo r1 = new RorCInfo();
//        r1.setIndex(0);
//        r1.setElement_type("1");
//        r1.setElement_name("月");
//        col_dim_list.add(r1);
//
//        for (int i=1; i < ini_col_size - 1; i++){
//            RorCInfo r = new RorCInfo();
//            r.setIndex(i);
//            r.setElement_type("1");
//            r.setElement_name("月"+ i);
//            col_dim_list.add(r);
//        }
//
//        RorCInfo rx = new RorCInfo();
//        rx.setIndex(ini_col_size - 1);
//        rx.setElement_type("2");
//        rx.setElement_name("值");
//        col_dim_list.add(rx);
//        List<RorCInfo> row_dim_list = new ArrayList<>();


        //构建数据
        String[][] datas = new String[ini_row_size][ini_col_size];
        datas[0][0] = "201901";//月
        datas[0][1] = "591";//地市
        datas[0][2] = "101";//区
        datas[0][3] = "1";//消费金额
        datas[0][4] = "100";//使用人数

        datas[1][0] = "201901";
        datas[1][1] = "592";
        datas[1][2] = "201";
        datas[1][3] = "2";
        datas[1][4] = "200";

        datas[2][0] = "201901";
        datas[2][1] = "593";
        datas[2][2] = "301";
        datas[2][3] = "3";
        datas[2][4] = "300";

        datas[3][0] = "201902";
        datas[3][1] = "591";
        datas[3][2] = "101";
        datas[3][3] = "4";
        datas[3][4] = "400";

        datas[4][0] = "201902";
        datas[4][1] = "592";
        datas[4][2] = "201";
        datas[4][3] = "5";
        datas[4][4] = "500";

        datas[5][0] = "201903";
        datas[5][1] = "593";
        datas[5][2] = "301";
        datas[5][3] = "6";
        datas[5][4] = "600";

//        行信息初始化
        RorCInfo r1 = new RorCInfo();
        r1.setIndex(0);
        r1.setElement_type("1");
        r1.setElement_name("月");

        //行信息初始化
        RorCInfo r2 = new RorCInfo();
        r2.setIndex(1);
        r2.setElement_type("1");
        r2.setElement_name("市");

        //行信息初始化
        RorCInfo r3 = new RorCInfo();
        r3.setIndex(2);
        r3.setElement_type("1");
        r3.setElement_name("县");

        //行信息初始化
        RorCInfo r4 = new RorCInfo();
        r4.setIndex(3);
        r4.setElement_type("2");
        r4.setElement_name("消费金额");

        //行信息初始化
        RorCInfo r5 = new RorCInfo();
        r5.setIndex(4);
        r5.setElement_type("2");
        r5.setElement_name("使用人数");

//          -------------------------------------------------------------------------
        List<RorCInfo> col_dim_list = new ArrayList<>();
//        col_dim_list.add(r1);
//        col_dim_list.add(r2);
//        col_dim_list.add(r3);
//        col_dim_list.add(r4);
//        col_dim_list.add(r5);

//        指定 列信息
        List<RorCInfo> row_dim_list = new ArrayList<>();
//        row_dim_list.add(r1);
//        row_dim_list.add(r2);
//        row_dim_list.add(r3);
        row_dim_list.add(r4);
        row_dim_list.add(r5);
        long startTime = System.nanoTime();

        ReportReqBean reqBean = new ReportReqBean();
        reqBean.setDatas(datas);
        reqBean.setRowDimList(row_dim_list);
        reqBean.setColDimList(col_dim_list);
        reqBean.setNeedTitle(true);
        reqBean.setNeedTotal(true);
        reqBean.setSubtotalField(r2);
        List<RorCInfo> hideRorCList = new ArrayList<>();
//        hideRorCList.add(r1);
//        hideRorCList.add(r2);
        hideRorCList.add(r3);
        reqBean.setHiddenDimList(hideRorCList);
        ReportRspBean rspBean = transDataSet(reqBean);
        String[][] new_datas = rspBean.getDatas();
        long ms = (System.nanoTime() - startTime)/1000000;
        System.out.println(ms+"ms");
        for (String[] newData : new_datas) {
            for (String newDatum : newData) {
                System.out.print(newDatum + " ");
            }
            System.out.println("");
        }
        System.out.println(rspBean.getMerge_list());
    }


    public static ReportRspBean transDataSet(ReportReqBean bean) throws Exception{
        List<RorCInfo> hideRorCList = bean.getHiddenDimList();
        String [][] datas = bean.getDatas();
        List<RorCInfo> rowDimList = bean.getRowDimList();
        List<RorCInfo> colDimList = bean.getColDimList();
        boolean isNeedTitle = bean.isNeedTitle();
        RorCInfo subtotalField = bean.getSubtotalField();
        boolean isNeedTotal = bean.isNeedTotal();
        boolean isNeedMerge = true;

        int ini_col_size = 0;//数据-列的初始化
        int ini_row_size = 0;//数据-行的初始化
        if(datas != null){
            ini_col_size = datas[0].length;
            ini_row_size = datas.length;
        }

        int target_size = 0;//  度量数量
        int row_dim_size = 0;//行纬度数量
        int col_dim_size = 0;//列纬度数量
        boolean is_target_inCol = false;//指标是否在列

        //生产情况需要设置index
        int tempIndex = 0;
        for (RorCInfo rorCInfo : colDimList) {
            if (null == rorCInfo) {
                throw new Exception("rorCInfo不能为空");
            }
            if (rorCInfo.getIndex() == null){
                rorCInfo.setIndex(tempIndex++);
            }
            if ("1".equals(rorCInfo.getElement_type())) {
                //纬度设置
                col_dim_size++;
            } else if ("2".equals(rorCInfo.getElement_type())) {
                //指标设置
                target_size++;
                is_target_inCol = true;
            } else {
                throw new Exception("元素类型不能为空");
            }
        }


        for (RorCInfo rorCInfo : rowDimList) {
            if (null == rorCInfo) {
                throw new Exception("rorCInfo不能为空");
            }
            if (rorCInfo.getIndex() == null){
                rorCInfo.setIndex(tempIndex++);
            }
            if ("1".equals(rorCInfo.getElement_type())) {
                //纬度设置
                row_dim_size++;
            } else if ("2".equals(rorCInfo.getElement_type())) {
                //指标设置
                target_size++;
                if (is_target_inCol == true) {
                    throw new Exception("度量不能分别存在于列和行");
                }
            } else {
                throw new Exception("元素类型不能为空");
            }
        }


        Map<String, String> dataMap = new HashMap<>();
        //设置行列对应的纬度信息  x = j ;  y = i;
        for (int i = 0; i < ini_row_size; i++) {
            for (int j = 0; j < ini_col_size; j++) {
                dataMap.put(j + ":" + i, datas[i][j]);
            }
        }

        //判断行和列的纬度信息
        //列map
        Map<String, Integer> colDistinctMap = new HashMap<>();
        Map<String, Integer> colMap = new LinkedHashMap<>();
        List<List<String>> colList = new ArrayList<>();
        //行map
        Map<String, Integer> rowDistinctMap = new HashMap<>();
        Map<String, Integer> rowMap = new LinkedHashMap<>();
        List<List<String>> rowList = new ArrayList<>();

        int col_index = 0;
        int row_index = 0;
        for (int i = 0; i < ini_row_size; i++) {
            StringBuilder sb_col = new StringBuilder();
            StringBuilder sb_row = new StringBuilder();

            int countForFirst = 0;//计算是否第一次
            List<String> tempList_col = new ArrayList<>();//纬度数据
            for (RorCInfo rorCInfo : colDimList) {
                String element_type = rorCInfo.getElement_type();
                int col_i = rorCInfo.getIndex();
                if ("1".equals(element_type)) {
                    if (0 != countForFirst) {
                        sb_col.append("|");

                    }
                    sb_col.append(dataMap.get(col_i + ":" + i));
                    tempList_col.add(dataMap.get(col_i + ":" + i));//设置纬度数据
                    countForFirst++;
                }
            }

            countForFirst = 0;
            List<String> tempList_row = new ArrayList<>();//纬度数据
            for (RorCInfo rorCInfo : rowDimList) {
                String element_type = rorCInfo.getElement_type();
                int row_i = rorCInfo.getIndex();
                if ("1".equals(element_type)) {
                    if (0 != countForFirst) {
                        sb_row.append("|");
                    }
                    sb_row.append(dataMap.get(row_i + ":" + i));
                    tempList_row.add(dataMap.get(row_i + ":" + i));//设置纬度数据
                    countForFirst++;
                }
            }

            if (!colDistinctMap.containsKey(sb_col.toString())) {

                //按顺序，判断去重， 在行的度量，列要乘以度量数量，值要转为md5或者hash，防止冲突，或者分隔符要重新定义
                if (is_target_inCol) {
                    colMap.put((sb_col.toString()), col_index++);
                    colList.add(tempList_col);//列的纬度数据赋值
                } else if (target_size > 0 || col_dim_size > 0){
                    for (int j = 1; j <= (target_size == 0 ? 1 : target_size); j++) {
                        colMap.put((sb_col.toString() + "|target" + j), col_index++);
                        colList.add(tempList_col);//列的纬度数据赋值
                    }
                }
                colDistinctMap.put(sb_col.toString(), 0);
            }

            if (!rowDistinctMap.containsKey(sb_row.toString())) {
                if (is_target_inCol) {
                    for (int j = 1; j <= (target_size == 0 ? 1 : target_size); j++) {
                        rowMap.put((sb_row.toString() + "|target" + j), row_index++);
                        rowList.add(tempList_row);//列的纬度数据赋值
                    }

                } else if (target_size > 0 || row_dim_size > 0){
                    rowMap.put(sb_row.toString(), row_index++);
                    rowList.add(tempList_row);//列的纬度数据赋值
                }

                rowDistinctMap.put(sb_row.toString(), 0);
            }
        }

        //隐藏维度偏移
        List<Integer> rowHideIndexList = new ArrayList<>();
        List<Integer> colHideIndexList = new ArrayList<>();
        for (RorCInfo rorCInfo : hideRorCList) {
            int rowHideIndex = rowDimList.indexOf(rorCInfo);
            if (rowHideIndex != -1){
                rowHideIndexList.add(rowHideIndex);
                continue;
            }
            int colHideIndex = colDimList.indexOf(rorCInfo);
            if (colHideIndex != -1) colHideIndexList.add(colHideIndex);
        }
        System.out.println("rowHideIndexList:"+rowHideIndexList.toString());
        System.out.println("colHideIndexList:"+colHideIndexList.toString());
        int row_dim_size_h = row_dim_size - rowHideIndexList.size();
        int col_dim_size_h = col_dim_size - colHideIndexList.size();

        //表头偏移量
        int col_offset =  0;
        int row_offset =  0;
        if(isNeedTitle){
            col_offset =  (!is_target_inCol && target_size > 0) || row_dim_size - rowHideIndexList.size() != 0 ? 1 :0;
            row_offset =  (is_target_inCol && target_size > 0) || col_dim_size - colHideIndexList.size() != 0 ? 1 :0;
        }

        //小计
        int col_subIndex = colDimList.indexOf(subtotalField);
        int row_subIndex = rowDimList.indexOf(subtotalField);
        List<Integer> sub_col_index = new ArrayList<>();
        List<Integer> sub_row_index = new ArrayList<>();
        int y_col_index = row_dim_size_h+row_offset;//y轴的行扩展
        int x_row_index = col_dim_size_h+col_offset;//x轴的列扩展
        String tempString = null;
        for (int n = 0, size = colList.size(); n < size; n++) {
            List<String> list = colList.get(n);
            if(col_subIndex != -1){
                if(!list.get(col_subIndex).equals(tempString) && tempString != null){
                    sub_col_index.add(y_col_index);
                    y_col_index++;
                    col_index++;
                }
                tempString = list.get(col_subIndex);
            }
            y_col_index++;
            if(col_subIndex != -1 && n == size -1){
                sub_col_index.add(y_col_index);
                y_col_index++;
                col_index++;
            }
        }
        tempString = null;
        for (int n = 0, size = rowList.size(); n < size; n ++) {
            List<String> list = rowList.get(n);
            if (row_subIndex != -1 ){
                if(!list.get(row_subIndex).equals(tempString) && tempString != null){
                    sub_row_index.add(x_row_index);
                    x_row_index++;
                    row_index++;
                }
                tempString = list.get(row_subIndex);
            }
            x_row_index++;
            if(row_subIndex != -1 && n == size -1){
                sub_row_index.add(x_row_index);
                x_row_index++;
                row_index++;
            }
        }

        //总计
        if(isNeedTotal){
            isNeedTotal = false;
            if(row_subIndex != -1){
                //合计在右方
                row_index++;
                isNeedTotal = true;
            }else if(col_subIndex != -1){
                //合计在下方
                col_index++;
                isNeedTotal = true;
            }
        }

        if(row_index + col_dim_size_h + col_offset > 50){
            throw new Exception("数据区域超过设计范围");
        }
        String[][] newDatas = new String[col_index + row_dim_size_h + row_offset][row_index + col_dim_size_h + col_offset];

        //添加表头
        if(isNeedTitle){
            for (int i = 0; i < row_dim_size; i++){
                if (rowHideIndexList.contains(i)) continue;
                int temp = i;
                for (Integer integer : rowHideIndexList) {
                    if (i >= integer){
                        temp--;
                    }else{
                        break;
                    }
                }
                newDatas[temp+row_offset][0] = rowDimList.get(i).getElement_name();
            }
            for (int j = 0; j < col_dim_size; j++){
                if (colHideIndexList.contains(j)) continue;
                int temp = j;
                for (Integer integer : colHideIndexList) {
                    if (j >= integer){
                        temp--;
                    }else{
                        break;
                    }
                }
                newDatas[0][temp+col_offset] = colDimList.get(j).getElement_name();
            }

            if (is_target_inCol){
                for (int j = col_dim_size_h + col_offset; j < row_index + col_dim_size_h + col_offset - (isNeedTotal?1:0);){
                    if(sub_row_index.contains(j)){
                        newDatas[0][j] = "小计";
                        j++;
                        continue;
                    }
                    for (int n=0; n<target_size;n++){
                        newDatas[0][j+n] = colDimList.get(col_dim_size+n).getElement_name();
                    }
                    j = j+target_size;
                }
            }else if (target_size > 0){
                for (int j = row_dim_size_h + row_offset; j < col_index + row_dim_size_h + row_offset - (isNeedTotal?1:0);){
                    if(sub_col_index.contains(j)){
                        newDatas[j][0] = "小计";
                        j++;
                        continue;
                    }
                    for (int n=0; n<target_size;n++){
                        newDatas[j+n][0] = rowDimList.get(row_dim_size+n).getElement_name();
                    }
                    j = j+target_size;
                }
            }
        }

        //填写维度的值
        y_col_index = row_dim_size_h+row_offset;//y轴的行扩展
        x_row_index = col_dim_size_h+col_offset;//x轴的列扩展
        for (int n = 0, size = colList.size(); n < size; n++) {
            List<String> list = colList.get(n);
            if(sub_col_index.contains(y_col_index)){
                for (int i = 0; i < list.size() - colHideIndexList.size(); i++) {
                    newDatas[(y_col_index)][(i+col_offset)] = "小计";
                }
                y_col_index++;
            }
            for (int i = 0; i < list.size(); i++) {
                if (colHideIndexList.contains(i)) continue;
                int temp = i;
                for (Integer integer : colHideIndexList) {
                    if (i >= integer){
                        temp--;
                    }else{
                        break;
                    }
                }
                newDatas[y_col_index][temp+col_offset] = list.get(i);
            }
            y_col_index++;
            if(col_subIndex != -1 && n == size -1){
                for (int i = 0; i < list.size() - colHideIndexList.size(); i++) {
                    newDatas[y_col_index][i+col_offset] = "小计";
                }
            }
        }
        for (int n = 0, size = rowList.size(); n < size; n ++) {
            List<String> list = rowList.get(n);
            if(sub_row_index.contains(x_row_index)){
                for (int i = 0; i < list.size() - rowHideIndexList.size(); i++) {
                    newDatas[i+row_offset][x_row_index] = "小计";
                }
                x_row_index++;
            }
            for (int i = 0; i < list.size(); i++) {
                if (rowHideIndexList.contains(i)) continue;
                int temp = i;
                for (Integer integer : rowHideIndexList) {
                    if (i >= integer){
                        temp--;
                    }else{
                        break;
                    }
                }
                newDatas[temp+row_offset][x_row_index] = list.get(i);
            }
            x_row_index++;
            if(row_subIndex != -1 && n == size -1){
                for (int i = 0; i < list.size() - rowHideIndexList.size(); i++) {
                    newDatas[i+row_offset][x_row_index] = "小计";
                }
            }
        }

        //填写度量的值
        for (int i = 0; i < ini_row_size; i++) {
            StringBuilder sb_col = new StringBuilder();
            StringBuilder sb_row = new StringBuilder();
            int target_index = 0;
            Map<String, Integer> targetTempMap = new LinkedHashMap<>();

            int countForFirst = 0;//计算是否第一次
            for (RorCInfo rorCInfo : colDimList) {
                String element_type = rorCInfo.getElement_type();
                int col_i = rorCInfo.getIndex();
                if ("1".equals(element_type)) {
                    if (0 != countForFirst) {
                        sb_col.append("|");
                    }
                    sb_col.append(dataMap.get(col_i + ":" + i));
                    countForFirst++;
                } else if ("2".equals(element_type)) {
                    //设置指标信息
                    targetTempMap.put(("target" + (++target_index)), col_i);
                }
            }

            countForFirst = 0;
            for (RorCInfo rorCInfo : rowDimList) {
                String element_type = rorCInfo.getElement_type();
                int row_i = rorCInfo.getIndex();
                if ("1".equals(element_type)) {
                    if (0 != countForFirst) {
                        sb_row.append("|");
                    }
                    sb_row.append(dataMap.get(row_i + ":" + i));
                    countForFirst++;
                } else if ("2".equals(element_type)) {
                    //设置指标信息
                    targetTempMap.put(("target" + (++target_index)), row_i);
                }
            }

            if (!targetTempMap.isEmpty()) {
                if (is_target_inCol) {
                    //如果度量在列
                    int y = 0;
                    if(StringUtils.isEmpty(sb_col)){
                        y = row_dim_size_h + row_offset;
                    }else{
                        y = colMap.get((sb_col.toString())) + row_dim_size_h + row_offset;
                    }
                    //小计位置偏移
                    for (Integer index : sub_col_index) {
                        if (y >= index){
                            y++;
                        }else{
                            break;
                        }
                    }

                    int temp_index = 0;
                    for (String key : targetTempMap.keySet()) {

                        int x = 0;
                        if(StringUtils.isEmpty(sb_row)){
                            x = col_dim_size_h + col_offset +(temp_index++);
                        }else{
                            x = rowMap.get(sb_row.toString() + "|" + key) + col_dim_size_h + col_offset;
                        }
                        //小计位置偏移
                        for (Integer index : sub_row_index) {
                            if (x >= index){
                                x++;
                            }else{
                                break;
                            }
                        }
                        String targetValue = dataMap.get(targetTempMap.get(key) + ":" + i);
                        newDatas[y][x] = targetValue;
                    }

                } else {
                    //如果度量在行
                    int x = 0;
                    if(StringUtils.isEmpty(sb_row)){
                        x = col_dim_size_h + col_offset;
                    }else{
                        x = rowMap.get(sb_row.toString()) + col_dim_size_h + col_offset;
                    }
                    //小计位置偏移
                    for (Integer index : sub_row_index) {
                        if (x >= index){
                            x++;
                        }else{
                            break;
                        }
                    }

                    int temp_index = 0;
                    for (String key : targetTempMap.keySet()) {
                        int y = 0;
                        if(StringUtils.isEmpty(sb_col)){
                            y = row_dim_size_h + row_offset +(temp_index++);
                        }else{
                            y = colMap.get((sb_col.toString() + "|" + key)) + row_dim_size_h + row_offset;
                        }
                        //小计位置偏移
                        for (Integer index : sub_col_index) {
                            if (y >= index){
                                y++;
                            }else{
                                break;
                            }
                        }
                        String targetValue = dataMap.get(targetTempMap.get(key) + ":" + i);
                        newDatas[y][x] = targetValue;
                    }
                }
            }
        }

        //计算小计的值
        int tempCol = row_dim_size_h+row_offset;
        for (Integer index : sub_col_index) {
            for (int i = 0; i< row_index; i++){
                int col = col_dim_size_h+col_offset+i;
                int subSum = 0;
                for (int j = tempCol; j < index; j++){
                    String result = newDatas[j][col];
                    if(result != null){
                        subSum = subSum + Integer.parseInt(result);
                    }
                }
                newDatas[index][col] = subSum+"";
            }
            tempCol = index + 1;
        }
        int tempRow = col_dim_size_h+col_offset;
        for (Integer index : sub_row_index) {
            for (int i = 0; i< col_index; i++){
                int row = row_dim_size_h+row_offset+i;
                int subSum = 0;
                for (int j = tempRow; j < index; j++){
                    String result = newDatas[row][j];
                    if(result != null){
                        subSum = subSum + Integer.parseInt(result);
                    }
                }
                newDatas[row][index] = subSum+"";
            }
            tempRow = index + 1;
        }
        //总计
        if(isNeedTotal){
            if(row_subIndex != -1){
                //合计在右方
                for (int i=is_target_inCol?0:row_offset; i < row_dim_size_h + row_offset; i++){
                    newDatas[i][col_offset+col_dim_size_h+row_index-1] = "总计";
                }
                int temp = row_dim_size_h + row_offset;
                for (int i=0; i < col_index; i++){
                    int sum = 0;
                    for (Integer index : sub_row_index) {
                        sum = sum + Integer.parseInt(newDatas[temp][index]);
                    }
                    newDatas[temp++][col_offset+col_dim_size_h+row_index-1] = sum+"";
                }
            }else if (col_subIndex != -1){
                //合计在下方

                for (int i=is_target_inCol?col_offset:0; i < col_dim_size_h + col_offset; i++){
                    newDatas[row_offset+row_dim_size_h+col_index-1][i] = "总计";
                }
                int temp = col_dim_size_h + col_offset;
                for (int i=0; i < row_index; i++){
                    int sum = 0;
                    for (Integer index : sub_col_index) {
                        sum = sum + Integer.parseInt(newDatas[index][temp]);
                    }
                    newDatas[row_offset+row_dim_size_h+col_index-1][temp++] = sum+"";
                }
            }
        }

        System.out.println("row_offset:" + row_offset);
        System.out.println("col_offset:" + col_offset);
        System.out.println("row_dim_size_h:" + row_dim_size_h);
        System.out.println("col_dim_size_h:" + col_dim_size_h);
        System.out.println("sub_row_index:" + sub_row_index);
        System.out.println("sub_row_index:" + sub_col_index);

        List<String> mergeInfoList = new ArrayList<>();
        //合并单元格
        if(isNeedMerge){
            for (Integer index : sub_col_index) {
                if(col_offset+col_dim_size_h > 1)
                mergeInfoList.add(JSON.toJSONString(getMergeInfo(index,0,0,col_offset+col_dim_size_h-1)));
            }
            for (Integer index : sub_row_index) {
                if(row_offset+row_dim_size_h > 1)
                mergeInfoList.add(JSON.toJSONString(getMergeInfo(0,index, row_offset+row_dim_size_h-1, 0)));
            }
            if(isNeedTotal){
                if(row_subIndex != -1){
                    //合计在右方
                    if(row_offset+row_dim_size_h-(is_target_inCol?0:row_offset) > 1)
                        mergeInfoList.add(JSON.toJSONString(getMergeInfo(is_target_inCol?0:row_offset,col_offset+col_dim_size_h+row_index-1, row_offset+row_dim_size_h-1-(is_target_inCol?0:row_offset), 0)));
                } else if (col_subIndex != -1){
                    //合计在下方
                    if(col_offset+col_dim_size_h-(is_target_inCol?col_offset:0) > 1)
                        mergeInfoList.add(JSON.toJSONString(getMergeInfo(row_offset+row_dim_size_h+col_index-1,is_target_inCol?col_offset:0, col_offset+col_dim_size_h-1-(is_target_inCol?col_offset:0), 0)));
                }
            }


            for(int i=0; i<row_offset+row_dim_size_h; i++){
                String temp = null;
                int row = i;
                int col = 0;
                for (int j=0,length=newDatas[0].length; j<length; j++){

                    if (temp == null && newDatas[i][j] == null){
                        col = j;
                        temp = newDatas[i][j];
                        continue;
                    }
                    if (temp == null || !temp.equals(newDatas[i][j])){
                        if(j - col > 1){
                            mergeInfoList.add(JSON.toJSONString(getMergeInfo(row,col, 0, j-col-1)));
                        }
                        col = j;
                    }
                    temp = newDatas[i][j];
                }
                if(newDatas[0].length - col > 1){
                    mergeInfoList.add(JSON.toJSONString(getMergeInfo(row,col, 0, newDatas[0].length-col-1)));
                }
            }

            for(int i=0; i<col_offset+col_dim_size_h; i++){
                String temp = null;
                int row = 0;
                int col = i;
                for (int j=0,length=newDatas.length; j<length; j++){

                    if (temp == null && newDatas[j][i] == null){
                        row = j;
                        temp = newDatas[j][i];
                        continue;
                    }
                    if (temp == null || !temp.equals(newDatas[j][i])){
                        if(j - row > 1){
                            mergeInfoList.add(JSON.toJSONString(getMergeInfo(row,col, j-row-1, 0)));
                        }
                        row = j;
                    }
                    temp = newDatas[j][i];
                }
                if(newDatas.length - row > 1){
                    mergeInfoList.add(JSON.toJSONString(getMergeInfo(row,col, newDatas.length-row-1, 0)));
                }
            }
        }

        ReportRspBean rspBean = new ReportRspBean();
        rspBean.setDatas(newDatas);
        rspBean.setMerge_list(mergeInfoList);

        return rspBean;
    }

    private static Map<String,Integer> getMergeInfo(int row, int col, int rowspan, int colspan){
        Map<String,Integer> mergeInfo = new LinkedHashMap<>();
        mergeInfo.put("row",row);
        mergeInfo.put("col",col);
        mergeInfo.put("rowspan",rowspan);
        mergeInfo.put("colspan",colspan);
        return mergeInfo;
    }
}
