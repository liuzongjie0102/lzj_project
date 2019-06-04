package com.lzj.test;

import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class Tmain2 {

    public static void main(String[] args) throws Exception {

        int ini_col_size = 5;//数据-列的初始化
        int ini_row_size = 6;//数据-行的初始化

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

        //行信息初始化
        RorCInfo r3 = new RorCInfo();
        r3.setIndex(0);
        r3.setElement_type("1");
        r3.setElement_name("月");

        //行信息初始化
        RorCInfo r1 = new RorCInfo();
        r1.setIndex(1);
        r1.setElement_type("1");
        r1.setElement_name("市");

        //行信息初始化
        RorCInfo r2 = new RorCInfo();
        r2.setIndex(2);
        r2.setElement_type("1");
        r2.setElement_name("县");

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


//-------------------------------------------------------------------------
        List<RorCInfo> col_dim_list = new ArrayList<>();
//        col_dim_list.add(r1);
        col_dim_list.add(r2);
        col_dim_list.add(r3);
//        col_dim_list.add(r4);
//        col_dim_list.add(r5);

        //指定 列信息
        List<RorCInfo> row_dim_list = new ArrayList<>();
        row_dim_list.add(r1);
//        row_dim_list.add(r2);
//        row_dim_list.add(r3);
        row_dim_list.add(r4);
        row_dim_list.add(r5);



        //====================数据初始化==========================

        int target_size = 0;//  度量数量
        int row_dim_size = 0;//行纬度数量
        int col_dim_size = 0;//列纬度数量
        boolean is_target_inCol = false;//指标是否在列

        for (RorCInfo rorCInfo : col_dim_list) {
            if (null == rorCInfo) {
                throw new Exception("rorCInfo不能为空");
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


        for (RorCInfo rorCInfo : row_dim_list) {
            if (null == rorCInfo) {
                throw new Exception("rorCInfo不能为空");
            }

            if ("1".equals(rorCInfo.getElement_type())) {
                //纬度设置
                row_dim_size++;

            } else if ("2".equals(rorCInfo.getElement_type())) {
                //指标设置
                target_size++;

                //
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
                System.out.print(datas[i][j]);
                System.out.print(" ");
                dataMap.put(j + ":" + i, datas[i][j]);
            }
            System.out.println();
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

        //全纬度对应的指标
        Map<String, Integer> targetMap = new HashMap<>();


        int col_index = 0;
        int row_index = 0;
        for (int i = 0; i < ini_row_size; i++) {
            StringBuilder sb_col = new StringBuilder();
            //StringBuilder sbWithTarget_col = new StringBuilder();
            StringBuilder sb_row = new StringBuilder();
            //StringBuilder sbWithTarget_row = new StringBuilder();

            int countForFirst = 0;//计算是否第一次
            List<String> tempList_col = new ArrayList<>();//纬度数据
            for (RorCInfo rorCInfo : col_dim_list) {

                String element_type = rorCInfo.getElement_type();
                int col_i = rorCInfo.getIndex();

                if ("1".equals(element_type)) {
                    if (0 != countForFirst) {
                        sb_col.append("|");
                    }

                    sb_col.append(dataMap.get(col_i + ":" + i));

                    tempList_col.add(dataMap.get(col_i + ":" + i));//设置纬度数据


                    countForFirst++;
                } else if ("2".equals(element_type)) {
                    //设置度量信息
                }
            }


            countForFirst = 0;
            List<String> tempList_row = new ArrayList<>();//纬度数据
            for (RorCInfo rorCInfo : row_dim_list) {

                String element_type = rorCInfo.getElement_type();
                int row_i = rorCInfo.getIndex();

                if ("1".equals(element_type)) {
                    if (0 != countForFirst) {
                        sb_row.append("|");
                    }

                    sb_row.append(dataMap.get(row_i + ":" + i));

                    tempList_row.add(dataMap.get(row_i + ":" + i));//设置纬度数据


                    countForFirst++;
                } else if ("2".equals(element_type)) {
                    //设置度量信息
                }
            }


            /**
             *
             */

            if (!colDistinctMap.containsKey(sb_col.toString())) {

                //按顺序，判断去重， 在行的度量，列要乘以度量数量，值要转为md5或者hash，防止冲突，或者分隔符要重新定义
                if (is_target_inCol) {
                    colMap.put((sb_col.toString()), col_index++);
                    colList.add(tempList_col);//列的纬度数据赋值
                } else {

                    for (int j = 1; j <= (target_size == 0 ? 1 : target_size); j++) {
                        if (is_target_inCol) {
                            //指标在列
                            colMap.put((sb_col.toString()), col_index++);
                        } else {
                            //指标在行
                            colMap.put((sb_col.toString() + "|target" + j), col_index++);
                        }

                        colList.add(tempList_col);//列的纬度数据赋值
                    }
                }

                colDistinctMap.put(sb_col.toString(), 0);
            }

            if (!rowDistinctMap.containsKey(sb_row.toString())) {
                //行的判断
//                rowMap.put(dataMap.get(0 + ":" + i), row_index++);
//
//                List<String> tempList = new ArrayList<>();
//                tempList.add(dataMap.get(0 + ":" + i));
//                rowList.add(tempList);


                if (is_target_inCol) {

                    for (int j = 1; j <= (target_size == 0 ? 1 : target_size); j++) {
                        if (is_target_inCol) {
                            //指标在列

                            rowMap.put((sb_row.toString() + "|target" + j), row_index++);
                        } else {
                            //指标在行
                            rowMap.put((sb_row.toString()), row_index++);
                        }

                        rowList.add(tempList_row);//列的纬度数据赋值
                    }

                } else {
                    rowMap.put(sb_row.toString(), row_index++);
                    rowList.add(tempList_row);//列的纬度数据赋值
                }

                rowDistinctMap.put(sb_row.toString(), 0);
            }
        }


        //结果Map
        Map<String, String> resultMap = new HashMap<>();

        int x_row_index = col_dim_size;//x轴的列扩展
        int y_col_index = row_dim_size;//y轴的行扩展
        for (List<String> list : colList) {
            for (int i = 0; i < list.size(); i++) {
                resultMap.put((i + ":" + y_col_index), list.get(i));
            }
            y_col_index++;
        }

        for (List<String> list : rowList) {
            for (int i = 0; i < list.size(); i++) {
                resultMap.put((x_row_index + ":" + i), list.get(i));
            }
            x_row_index++;
        }


        for (int i = 0; i < ini_row_size; i++) {

            StringBuilder sb_col = new StringBuilder();
            StringBuilder sb_row = new StringBuilder();

            int target_index = 0;
            Map<String, Integer> targetTempMap = new LinkedHashMap<>();

            int countForFirst = 0;//计算是否第一次

            for (RorCInfo rorCInfo : col_dim_list) {

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

            for (RorCInfo rorCInfo : row_dim_list) {

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
                        y = row_dim_size;
                    }else{
                        y = colMap.get((sb_col.toString())) + row_dim_size;
                    }

                    int temp_index = 0;
                    for (String key : targetTempMap.keySet()) {

                        int x = 0;

                        if(StringUtils.isEmpty(sb_row)){
                            x = col_dim_size +(temp_index++);
                        }else{
                            x = rowMap.get(sb_row.toString() + "|" + key) + col_dim_size;
                        }

                        String targetValue = dataMap.get(targetTempMap.get(key) + ":" + i);
                        resultMap.put((x + ":" + y), targetValue);
                    }

                } else {

                    //如果度量在行
                    int x = 0;
                    if(StringUtils.isEmpty(sb_row)){
                        x = col_dim_size;
                    }else{
                        x = rowMap.get(sb_row.toString()) + col_dim_size;
                    }


                    int temp_index = 0;
                    for (String key : targetTempMap.keySet()) {
                        int y = 0;
                        if(StringUtils.isEmpty(sb_col)){
                            y = row_dim_size +(temp_index++);
                        }else{
                            y = colMap.get((sb_col.toString() + "|" + key)) + row_dim_size;
                        }

                        String targetValue = dataMap.get(targetTempMap.get(key) + ":" + i);
                        resultMap.put((x + ":" + y), targetValue);
                    }


                }

            }


//            String target1 = dataMap.get(3 + ":" + i);
//            String target2 = dataMap.get(4 + ":" + i);
//
//            int y = colMap.get((sb_col.toString() + "|target1")) + row_dim_size;
//            int y2 = colMap.get((sb_col.toString() + "|target2")) + row_dim_size;
//
//
//            int x = rowMap.get(sb_row.toString()) + col_dim_size;
//
//
//
//            resultMap.put(x + ":" + y, target1);
//            resultMap.put(x + ":" + y2, target2);

        }

        int col_offset =  0;
        int row_offset =  0;
        //增加表头
        if(true){
            col_offset =  !is_target_inCol || row_dim_size != 0 ? 1 :0;
            row_offset =  is_target_inCol || col_dim_size != 0 ? 1 :0;
            Map<String, String> resultTitleMap = new HashMap<>();
            for (int i = 0; i < row_dim_size; i++){
                resultTitleMap.put( 0 + ":" + (i+row_offset), row_dim_list.get(i).getElement_name() );
            }
            for (int j = 0; j < col_dim_size; j++){
                resultTitleMap.put( (j+col_offset) + ":" + 0, col_dim_list.get(j).getElement_name() );
            }
            if (is_target_inCol){
                for (int j = col_dim_size; j < row_index + col_dim_size + col_offset;){
                    for (int n=0; n<target_size;n++){
                        resultTitleMap.put( (j+n+col_offset) + ":" + 0,
                                        col_dim_list.get(col_dim_size+n).getElement_name());
                    }
                    j = j+target_size;
                }
            }else {
                for (int j = row_dim_size; j < col_index + row_dim_size + row_offset;){
                    for (int n=0; n<target_size;n++){
                        resultTitleMap.put( 0 + ":" + (j+n+row_offset),
                                        row_dim_list.get(row_dim_size+n).getElement_name());
                    }
                    j = j+target_size;
                }
            }

            for (int i = row_offset; i < col_index + row_dim_size + row_offset; i++) {
                for (int j = col_offset; j < row_index + col_dim_size + col_offset; j++) {
                    resultTitleMap.put( j + ":" + i , resultMap.get((j - col_offset) + ":" + (i - row_offset)));
                }
            }
            resultMap = resultTitleMap;
        }


        System.out.println("======================");

        for (int i = 0; i < col_index + row_dim_size + row_offset; i++) {

            for (int j = 0; j < row_index + col_dim_size + col_offset; j++) {
                System.out.print(">" + resultMap.get(j + ":" + i));
                System.out.print(" ");
            }

            System.out.println("");
        }

    }
}
