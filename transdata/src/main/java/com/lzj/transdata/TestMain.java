package com.lzj.transdata;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TestMain {
    public static void main(String[] args) {
        Path file = Paths.get("info.txt");
        List<DataModel> dataModels = new ArrayList<>();

        try {
            List<String> lines = Files.readAllLines(file);
            Map<Integer, List<String>> dataMap = new HashMap<>();
            int count = 0;
            for (String line : lines) {
                if (StringUtils.isNotBlank(line)){
                    if (StringUtils.startsWith(line, "*")){
                        count++;
                        List<String> group = new ArrayList<>();
                        dataMap.put(count, group);
                    }
                    dataMap.get(count).add(line);
                }
            }

            List<String> moneyLines = Files.readAllLines(Paths.get("money.txt"));
            List<Double> moneyList = new ArrayList<>();
            for (String moneyLine : moneyLines) {
                if (StringUtils.isNotBlank(moneyLine)){
                    moneyLine = StringUtils.replace(moneyLine, " ", "");
                    int index;
                    while ((index = moneyLine.indexOf(".")) > -1){
                        String temp = StringUtils.substring(moneyLine, 0, index + 3);
                        String[] substr = StringUtils.split(temp, ".");
                        if (substr.length != 2 || substr[1].length() != 2){
                            throw new Exception("数据有误：" + temp + "没有两位小数");
                        }
                        moneyList.add(Double.parseDouble(temp));
                        moneyLine = StringUtils.substring(moneyLine, index + 3);
                    }
                }
            }

            if (dataMap.size() != moneyList.size()){
                throw new Exception("数据有误：info size " + dataMap.size() + ", money size " + moneyList.size());
            }

            for (Map.Entry<Integer, List<String>> entry : dataMap.entrySet()) {
                List<String> temp = entry.getValue();
                DataModel model = new DataModel();
                String[] str = StringUtils.split(temp.get(0), "*");
                model.setGoodsSeq(entry.getKey());
                model.setGoodsClass(str[0]);
                model.setGoodsName(str[1]);
                model.setGoodsMoney(moneyList.get(entry.getKey() - 1));
                if (temp.size() == 3){
                    model.setGoodsType(temp.get(1));
                    model.setGoodsUnit(temp.get(2));
                }else if (temp.size() == 2){
                    model.setGoodsUnit(temp.get(1));
                }else{
                    for (String s : temp) {
                        System.out.println(s);
                    }
                    throw new Exception("第" + entry.getKey() + "行数据异常");
                }
                dataModels.add(model);
            }

            List<String> result = new ArrayList<>();
            for (DataModel dataModel : dataModels) {
//                System.out.println(dataModel.getGoodsName() + "\t" + dataModel.getGoodsType() + "\t" + dataModel.getGoodsUnit() + "\t" + dataModel.getGoodsClass() + "\t" + dataModel.getGoodsMoney());
                result.add(dataModel.getGoodsName() + "\t"
                        + dataModel.getGoodsType() + "\t"
                        + dataModel.getGoodsUnit() + "\t"
                        + "\t"
                        + "\t"
                        + dataModel.getGoodsMoney() + "\t"
                        + dataModel.getGoodsClass() + "\t"
                        );
            }
            System.out.println("Total Count: " + count);
            System.out.println("Total Money: " + moneyList.stream().collect(Collectors.summingDouble(Double::doubleValue)));

            Path out = Paths.get("out.txt");
            Files.write(out, result, StandardOpenOption.CREATE);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (NumberFormatException e){
            e.printStackTrace();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
