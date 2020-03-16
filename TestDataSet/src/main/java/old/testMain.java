package old;

import com.alibaba.fastjson.JSON;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class testMain {

    public static void main(String[] args){
        int ini_col_size = 5;//数据-列的初始化
        int ini_row_size = 6;//数据-行的初始化

        //构建数据
        String[][] datas = new String[ini_row_size][ini_col_size];

        datas[0][0] = "201901";//月
        datas[0][1] = "591";//地市
        datas[0][2] = "101";//区
        datas[0][3] = "1";//消费金额
        datas[0][4] = "500";//使用人数

        datas[1][0] = "201901";
        datas[1][1] = "592";
        datas[1][2] = "201";
        datas[1][3] = "2";
        datas[1][4] = "500";

        datas[2][0] = "201901";
        datas[2][1] = "593";
        datas[2][2] = "101";
        datas[2][3] = "3";
        datas[2][4] = "500";

        datas[3][0] = "201902";
        datas[3][1] = "591";
        datas[3][2] = "101";
        datas[3][3] = "4";
        datas[3][4] = "500";

        datas[4][0] = "201902";
        datas[4][1] = "592";
        datas[4][2] = "201";
        datas[4][3] = "5";
        datas[4][4] = "500";

        datas[5][0] = "201903";
        datas[5][1] = "593";
        datas[5][2] = "301";
        datas[5][3] = "6";
        datas[5][4] = "500";

        List<String> mergeInfoList = new ArrayList<>();


        for(int i=0; i<ini_row_size; i++){
            String temp = null;
            int row = i;
            int col = 0;
            for (int j=0,length=datas[0].length; j<length; j++){

                if (temp == null && datas[i][j] == null){
                    col = j;
                    temp = datas[i][j];
                    continue;
                }
                if (temp == null || !temp.equals(datas[i][j])){
                    if(j - col > 1){
                        mergeInfoList.add(JSON.toJSONString(getMergeInfo(row,col, 0, j-col-1)));
                    }
                    col = j;
                }
                temp = datas[i][j];
            }
            if(datas[0].length - col > 1){
                mergeInfoList.add(JSON.toJSONString(getMergeInfo(row,col, 0, datas[0].length-col-1)));
            }
        }

        for(int i=0; i<ini_col_size; i++){
            String temp = null;
            int row = 0;
            int col = i;
            for (int j=0,length=datas.length; j<length; j++){

                if (temp == null && datas[j][i] == null){
                    row = j;
                    temp = datas[j][i];
                    continue;
                }
                if (temp == null || !temp.equals(datas[j][i])){
                    if(j - row > 1){
                        mergeInfoList.add(JSON.toJSONString(getMergeInfo(row,col, j-row-1, 0)));
                    }
                    row = j;
                }
                temp = datas[j][i];
            }
            if(datas.length - row > 1){
                mergeInfoList.add(JSON.toJSONString(getMergeInfo(row,col, datas.length-row-1, 0)));
            }
        }

        for (String[] data : datas) {
            for (String newDatum : data) {
                System.out.print(newDatum + " ");
            }
            System.out.println("");
        }

        System.out.println("mergeInfoList:"+mergeInfoList);



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
