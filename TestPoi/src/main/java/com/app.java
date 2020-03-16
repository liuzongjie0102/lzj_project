package com;

import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;

public class app {
    public static void main(String[] args) {
        try {
            new app().app();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void app() throws Exception {
        InputStream in = new FileInputStream(this.getClass().getResource("/dataassetModule.xlsx").getPath());
        XSSFWorkbook wb = new XSSFWorkbook(in);
        XSSFSheet sheet = wb.getSheetAt(0);
        XSSFRow row = sheet.getRow(0);
        System.out.println(row.getCell(0));
        wb.cloneSheet(0,"clone1");

        OutputStream out = new FileOutputStream("d://dataassetModule.xlsx");
        wb.write(out);
    }
}
