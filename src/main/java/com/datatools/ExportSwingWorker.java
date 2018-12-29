package com.datatools;

import com.datatools.bean.*;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.xssf.usermodel.*;

import javax.swing.*;
import java.io.FileOutputStream;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class ExportSwingWorker extends SwingWorker<Integer, Integer> {

    private ExportCallBack exportCallBack;
    private Connection connection;
    //年
    private String paramYear;
    //月
    private String paramMonth;
    //表日期
    private String paramDate;

    public ExportSwingWorker(String paramYear, String paramMonth, String paramDate, ExportCallBack exportCallBack) {
        this.paramYear = paramYear;
        this.paramMonth = paramMonth;
        this.paramDate = paramDate;
        this.exportCallBack = exportCallBack;
        connection = DBUtils.getConnection();
    }

    @Override
    protected Integer doInBackground() throws Exception {
        CallableStatement cs = null;
        CallableStatement cs1 = null;
        CallableStatement cs2 = null;
        CallableStatement cs3 = null;
        CallableStatement cs4 = null;
        CallableStatement cs5 = null;
        try {
            long s = System.currentTimeMillis();
            exportCallBack.onExportStart();
            exportCallBack.onExportProcess("正在检测导出环境...");
            //判断目标月份基础数据表是否存在，不在则提示
            cs = connection.prepareCall("{call proc_table_exists(?,?)}");
            cs.setInt(1, 1);
            cs.setString(2, paramDate);
            ResultSet rset = cs.executeQuery();
            if (rset.next()) {
                int res = rset.getInt(1);
                String msg = rset.getString(2);
                if (res == 1) {
                    exportCallBack.onExportError("导出失败，" + paramDate + msg + "数据尚未导入，请先导入基础数据");
                    return null;
                }
            } else {
                exportCallBack.onExportError("导出失败，请检查数据库是否可以正常连接");
                return null;
            }
            List<ViewTable1> viewTable1List = new ArrayList<ViewTable1>();
            List<ViewTable2> viewTable2List = new ArrayList<ViewTable2>();
            List<ViewTable3> viewTable3List = new ArrayList<ViewTable3>();
            List<ViewTable4> viewTable4List = new ArrayList<ViewTable4>();
            List<ViewTable5> viewTable5List = new ArrayList<ViewTable5>();

            //地市办卡统计，封装数据
            exportCallBack.onExportProcess("开始执行“地市办卡统计”报表(1/10)");
            cs1 = connection.prepareCall("{call proc_table1(?,?,?)}");
            cs1.setString(1, paramYear);
            cs1.setString(2, paramMonth);
            cs1.setString(3, paramDate);
            ResultSet rs = cs1.executeQuery();
            ViewTable1 viewTable1;
            while (rs.next()) {
                viewTable1 = new ViewTable1();
                viewTable1.setIndexid(rs.getInt(1));
                viewTable1.setEnterpriseoperateid(rs.getInt(2));
                viewTable1.setGasstation(rs.getString(3));
                viewTable1.setActivenum(rs.getInt(4));
                viewTable1.setActivecardnum(rs.getInt(5));
                viewTable1.setFivecardnum(rs.getInt(6));
                viewTable1.setFivenewnum(rs.getInt(7));
                viewTable1.setPriceactive(rs.getDouble(8));
                viewTable1.setPricefive(rs.getDouble(9));
                viewTable1.setStationcardnum(rs.getInt(10));
                viewTable1.setStationnewnum(rs.getInt(11));
                viewTable1.setOrgcardnum(rs.getInt(12));
                viewTable1.setOrgnewnum(rs.getInt(13));
                viewTable1.setPricestation(Double.parseDouble(rs.getString(14)));
                viewTable1.setPriceall(Double.parseDouble(rs.getString(15)));
                viewTable1.setStarttime(rs.getString(16));
                viewTable1.setEndtime(rs.getString(17));
                viewTable1List.add(viewTable1);
            }

            //地市奖励统计，封装数据
            exportCallBack.onExportProcess("开始执行“地市奖励统计”报表(2/10)");
            cs2 = connection.prepareCall("{call proc_table2(?,?,?)}");
            cs2.setString(1, paramYear);
            cs2.setString(2, paramMonth);
            cs2.setString(3, paramDate);
            ResultSet rs2 = cs2.executeQuery();
            ViewTable2 viewTable2;
            while (rs2.next()) {
                viewTable2 = new ViewTable2();
                viewTable2.setIndexid(rs2.getInt(1));
                viewTable2.setEnterpriseoperateid(rs2.getInt(2));
                viewTable2.setGasstation(rs2.getString(3));
                viewTable2.setFivecardnum(rs2.getInt(4));
                viewTable2.setFivecardreward(rs2.getInt(5));
                viewTable2.setStationcardnum(rs2.getInt(6));
                viewTable2.setStationcardreward(rs2.getInt(7));
                viewTable2.setQycust(rs2.getDouble(8));
                viewTable2.setQycustreward(rs2.getDouble(9));
                viewTable2.setQycusth3(rs2.getDouble(10));
                viewTable2.setQycustrewardh3(rs2.getDouble(11));
                viewTable2.setCycust(rs2.getDouble(12));
                viewTable2.setCycustreward(rs2.getDouble(13));
                viewTable2.setCycusth3(rs2.getDouble(14));
                viewTable2.setCycustrewardh3(rs2.getDouble(15));
                viewTable2.setRewardall(rs2.getDouble(16));
                viewTable2.setStarttime(rs2.getString(17));
                viewTable2.setEndtime(rs2.getString(18));
                viewTable2List.add(viewTable2);
            }

            //个人办卡消费统计，封装数据
            exportCallBack.onExportProcess("开始执行“个人办卡消费统计”报表(3/10)");
            cs3 = connection.prepareCall("{call proc_table3(?,?,?)}");
            cs3.setString(1, paramYear);
            cs3.setString(2, paramMonth);
            cs3.setString(3, paramDate);
            ResultSet rs3 = cs3.executeQuery();
            ViewTable3 viewTable3;

            while (rs3.next()) {
                viewTable3 = new ViewTable3();
                viewTable3.setIndexid(rs3.getInt(1));
                viewTable3.setEnterpriseoperateid(rs3.getInt(2));
                viewTable3.setGasstation(rs3.getString(3));
                viewTable3.setPersonid(rs3.getInt(4));
                viewTable3.setPersonname(rs3.getString(5));
                viewTable3.setPersonno(rs3.getString(6));
                viewTable3.setFivecardnum(rs3.getInt(7));
                viewTable3.setFivecardnumyx(rs3.getInt(8));
                viewTable3.setFivecardreward(rs3.getDouble(9));
                viewTable3.setStationcardnum(rs3.getInt(10));
                viewTable3.setStationcardnumyx(rs3.getInt(11));
                viewTable3.setStationcardreward(rs3.getDouble(12));
                viewTable3.setOrgcardnum(rs3.getInt(13));
                viewTable3.setOrgcardnumyx(rs3.getInt(14));
                viewTable3.setQycustq3(rs3.getDouble(15));
                viewTable3.setQycustrewardq3(rs3.getDouble(16));
                viewTable3.setQycusth3(rs3.getDouble(17));
                viewTable3.setQycustrewardh3(rs3.getDouble(18));
                viewTable3.setCycustq3_gr(rs3.getDouble(19));
                viewTable3.setCycusth3_gr(rs3.getDouble(20));
                viewTable3.setCycustq3(rs3.getDouble(21));
                viewTable3.setCycustrewardq3(rs3.getDouble(22));
                viewTable3.setCycusth3(rs3.getDouble(23));
                viewTable3.setCycustrewardh3(rs3.getDouble(24));
                viewTable3.setRewardall(rs3.getDouble(25));
                viewTable3.setStarttime(rs3.getString(26));
                viewTable3.setEndtime(rs3.getString(27));
                viewTable3List.add(viewTable3);
            }

            //个人办卡消费明细,封装数据
            exportCallBack.onExportProcess("开始执行“个人办卡消费明细”报表(4/10)");
            cs4 = connection.prepareCall("{call proc_table4(?,?,?)}");
            cs4.setString(1, paramYear);
            cs4.setString(2, paramMonth);
            cs4.setString(3, paramDate);
            ResultSet rs4 = cs4.executeQuery();
            ViewTable4 viewTable4;

            while (rs4.next()) {
                viewTable4 = new ViewTable4();
                viewTable4.setIndexid(rs4.getInt(1));
                viewTable4.setEnterpriseoperateid(rs4.getInt(2));
                viewTable4.setGasstation(rs4.getString(3));
                viewTable4.setPersonid(rs4.getLong(4));
                viewTable4.setPersonname(rs4.getString(5));
                viewTable4.setPersonno(rs4.getString(6));
                viewTable4.setCustomername(rs4.getString(7));
                viewTable4.setCardclass(rs4.getString(8));
                viewTable4.setCardtype(rs4.getString(9));
                viewTable4.setCardno(rs4.getString(10));
                viewTable4.setPaypricefirst(rs4.getDouble(11));
                viewTable4.setRecordtime(rs4.getTimestamp(12));
                viewTable4.setCustomerphone(rs4.getString(13));
                viewTable4.setQycust(rs4.getDouble(14));
                viewTable4.setCycust(rs4.getDouble(15));
                viewTable4.setFycust(rs4.getDouble(16));
                viewTable4.setFlag500(rs4.getString(17));
                viewTable4List.add(viewTable4);
            }

            //异常卡信息，封装数据
            exportCallBack.onExportProcess("开始执行“异常卡信息”报表(5/10)");
            cs5 = connection.prepareCall("{call proc_table5(?,?,?)}");
            cs5.setString(1, paramYear);
            cs5.setString(2, paramMonth);
            cs5.setString(3, paramDate);
            ResultSet rs5 = cs5.executeQuery();
            ViewTable5 viewTable5;
            while (rs5.next()) {
                viewTable5 = new ViewTable5();
                viewTable5.setIndexid(rs5.getInt(1));
                viewTable5.setEnterpriseoperateid(rs5.getInt(2));
                viewTable5.setGasstation(rs5.getString(3));
                viewTable5.setPersonid(rs5.getLong(4));
                viewTable5.setPersonname(rs5.getString(5));
                viewTable5.setPersonno(rs5.getString(6));
                viewTable5.setCustomername(rs5.getString(7));
                viewTable5.setCardclass(rs5.getString(8));
                viewTable5.setCardno(rs5.getString(9));
                viewTable5.setPhoneyz(rs5.getString(10));
                viewTable5.setPhonedatame(rs5.getString(11));
                viewTable5.setPhonedatathat(rs5.getString(12));
                viewTable5.setNoyz(rs5.getString(13));
                viewTable5.setNodatame(rs5.getString(14));
                viewTable5.setNodatathat(rs5.getString(15));
                viewTable5.setUkeyyz(rs5.getString(16));
                viewTable5.setUkeydatame(rs5.getString(17));
                viewTable5.setUkeydatathat(rs5.getString(18));
                viewTable5.setFlag(rs5.getString(19));
                viewTable5List.add(viewTable5);
            }

            //导入 地市办卡统计 到excel
            exportCallBack.onExportProcess("正在导出“地市办卡统计”报表(6/10)");
            //城市办卡统计Excel
            // 第一步，创建一个workbook，对应一个Excel文件
            XSSFWorkbook xssfWorkbook = new XSSFWorkbook();
            // 第二步，在webbook中添加一个sheet,对应Excel文件中的sheet
            XSSFSheet xssfSheet = xssfWorkbook.createSheet("地市办卡统计");
            // 第三步，在sheet中添加表头第0行,注意老版本poi对Excel的行数列数有限制short
            XSSFRow xssfRow = xssfSheet.createRow(0);
            // 第四步，创建单元格，并设置值表头 设置表头居中
            XSSFCellStyle xssfCellStyle = xssfWorkbook.createCellStyle();
            //居中样式
            xssfCellStyle.setAlignment(HorizontalAlignment.LEFT);

            String[] titles = new String[]{"索引号", "机构ID", "地市分公司", "有效活动数量", "卡系统指定五进网点实际发卡数量", "比对后有效五进发卡数量", "比对后有效五进新卡数量", "五进网点实际充值金额", "五进有效卡充值金额", "比对后有效站内发卡数量", "比对后有效站内新卡数量", "比对后有效站内机构客户数量", "比对后有效站内机构新客户数量", "站内充值金额", "小计金额", "统计开始时间", "统计结束时间"};
            XSSFCell xssfCell = null;
            for (int i = 0; i < titles.length; i++) {
                xssfCell = xssfRow.createCell(i);//列索引从0开始
                xssfCell.setCellValue(titles[i]);//列名1
                xssfCell.setCellStyle(xssfCellStyle);//列居中显示
            }

            if (viewTable1List != null && !viewTable1List.isEmpty()) {
                for (int i = 0; i < viewTable1List.size(); i++) {
                    xssfRow = xssfSheet.createRow(i + 1);
                    ViewTable1 cityStats = viewTable1List.get(i);
                    xssfRow.createCell(0).setCellValue(cityStats.getIndexid());//隐藏列
                    xssfRow.createCell(1).setCellValue(cityStats.getEnterpriseoperateid());//隐藏列
                    xssfRow.createCell(2).setCellValue(cityStats.getGasstation());
                    xssfRow.createCell(3).setCellValue(cityStats.getActivenum());
                    xssfRow.createCell(4).setCellValue(cityStats.getActivecardnum());
                    xssfRow.createCell(5).setCellValue(cityStats.getFivecardnum());
                    xssfRow.createCell(6).setCellValue(cityStats.getFivenewnum());
                    xssfRow.createCell(7).setCellValue(cityStats.getPriceactive());
                    xssfRow.createCell(8).setCellValue(cityStats.getPricefive());
                    xssfRow.createCell(9).setCellValue(cityStats.getStationcardnum());
                    xssfRow.createCell(10).setCellValue(cityStats.getStationnewnum());
                    xssfRow.createCell(11).setCellValue(cityStats.getOrgcardnum());
                    xssfRow.createCell(12).setCellValue(cityStats.getOrgnewnum());
                    xssfRow.createCell(13).setCellValue(cityStats.getPricestation());
                    xssfRow.createCell(14).setCellValue(cityStats.getPriceall());
                    xssfRow.createCell(15).setCellValue(cityStats.getStarttime());
                    xssfRow.createCell(16).setCellValue(cityStats.getEndtime());
                }
            }
            //隐藏未使用列
            xssfSheet.setColumnHidden(0, true);
            xssfSheet.setColumnHidden(1, true);

            //导入 城市奖励统计 Excel
            exportCallBack.onExportProcess("正在导出“地市奖励统计”报表(7/10)");
            // 第二步，在webbook中添加一个sheet,对应Excel文件中的sheet
            XSSFSheet xssfSheet1 = xssfWorkbook.createSheet("地市奖励统计");
            // 第三步，在sheet中添加表头第0行,注意老版本poi对Excel的行数列数有限制short
            XSSFRow xssfRow1 = xssfSheet1.createRow(0);
            // 第四步，创建单元格，并设置值表头 设置表头居中
            XSSFCellStyle xssfCellStyle1 = xssfWorkbook.createCellStyle();
            //居中样式
            xssfCellStyle1.setAlignment(HorizontalAlignment.LEFT);

            String[] titles1 = new String[]{"索引号", "机构ID", "地市分公司", "五进卡张数", "五进卡奖励", "站内卡张数", "站内卡奖励", "汽油消费(0-3个月)", "汽油消费奖励(0-3个月)", "汽油消费(3-6个月)", "汽油消费奖励(3-6个月)", "柴油消费(0-3个月)", "柴油消费奖励(0-3个月)", "柴油消费(3-6个月)", "柴油消费奖励(3-6个月)", "合计", "统计开始时间", "统计结束时间"};
            XSSFCell xssfCell1 = null;
            for (int i = 0; i < titles1.length; i++) {
                xssfCell1 = xssfRow1.createCell(i);//列索引从0开始
                xssfCell1.setCellValue(titles1[i]);//列名1
                xssfCell1.setCellStyle(xssfCellStyle1);//列居中显示
            }

            if (viewTable2List != null && !viewTable2List.isEmpty()) {
                for (int i = 0; i < viewTable2List.size(); i++) {
                    xssfRow1 = xssfSheet1.createRow(i + 1);
                    ViewTable2 cityStats1 = viewTable2List.get(i);
                    xssfRow1.createCell(0).setCellValue(cityStats1.getIndexid());//隐藏列
                    xssfRow1.createCell(1).setCellValue(cityStats1.getEnterpriseoperateid());//隐藏列
                    xssfRow1.createCell(2).setCellValue(cityStats1.getGasstation());
                    xssfRow1.createCell(3).setCellValue(cityStats1.getFivecardnum());
                    xssfRow1.createCell(4).setCellValue(cityStats1.getFivecardreward());
                    xssfRow1.createCell(5).setCellValue(cityStats1.getStationcardnum());
                    xssfRow1.createCell(6).setCellValue(cityStats1.getStationcardreward());
                    xssfRow1.createCell(7).setCellValue(cityStats1.getQycust());
                    xssfRow1.createCell(8).setCellValue(cityStats1.getQycustreward());
                    xssfRow1.createCell(9).setCellValue(cityStats1.getQycusth3());
                    xssfRow1.createCell(10).setCellValue(cityStats1.getQycustrewardh3());
                    xssfRow1.createCell(11).setCellValue(cityStats1.getCycust());
                    xssfRow1.createCell(12).setCellValue(cityStats1.getCycustreward());
                    xssfRow1.createCell(13).setCellValue(cityStats1.getCycusth3());
                    xssfRow1.createCell(14).setCellValue(cityStats1.getCycustrewardh3());
                    xssfRow1.createCell(15).setCellValue(cityStats1.getRewardall());
                    xssfRow1.createCell(16).setCellValue(cityStats1.getStarttime());
                    xssfRow1.createCell(17).setCellValue(cityStats1.getEndtime());
                }
            }
            //隐藏未使用列
            xssfSheet1.setColumnHidden(0, true);
            xssfSheet1.setColumnHidden(1, true);

            //导入 城市奖励统计 Excel
            exportCallBack.onExportProcess("正在导出“个人办卡消费统计”报表(8/10)");
            //个人办卡消费统计
            // 第二步，在webbook中添加一个sheet,对应Excel文件中的sheet
            XSSFSheet xssfSheet2 = xssfWorkbook.createSheet("个人办卡消费统计");
            // 第三步，在sheet中添加表头第0行,注意老版本poi对Excel的行数列数有限制short
            XSSFRow xssfRow2 = xssfSheet2.createRow(0);
            // 第四步，创建单元格，并设置值表头 设置表头居中
            XSSFCellStyle xssfCellStyle2 = xssfWorkbook.createCellStyle();
            //居中样式
            xssfCellStyle2.setAlignment(HorizontalAlignment.LEFT);

            String[] titles2 = new String[]{"索引号", "机构ID", "地市分公司", "员工ID", "员工姓名", "员工编号", "五进卡数量", "五进新卡有效数量", "五进卡奖励", "站内个人卡数量", "站内个人新卡有效数量", "站内卡奖励", "站内机构客户数量", "站内机构新客户有效数量", "汽油消费金额（0-3月）", "汽油消费奖励（0-3月)",
                    "汽油消费金额（3-6月）", "汽油消费奖励（3-6月）", "个人柴油消费金额（0-3月）", "个人柴油消费金额（3-6月）", "车队柴油消费金额（0-3月）", "车队柴油消费奖励（0-3月）", "车队柴油消费金额（3-6月）", "车队柴油消费奖励（3-6月）", "合计奖励金额", "统计开始时间", "统计结束时间"};
            XSSFCell xssfCell2 = null;
            for (int i = 0; i < titles2.length; i++) {
                xssfCell2 = xssfRow2.createCell(i);//列索引从0开始
                xssfCell2.setCellValue(titles2[i]);//列名1
                xssfCell2.setCellStyle(xssfCellStyle2);//列居中显示
            }

            if (viewTable3List != null && !viewTable3List.isEmpty()) {
                for (int i = 0; i < viewTable3List.size(); i++) {
                    xssfRow2 = xssfSheet2.createRow(i + 1);
                    ViewTable3 cityStats = viewTable3List.get(i);
                    xssfRow2.createCell(0).setCellValue(cityStats.getIndexid());//隐藏列
                    xssfRow2.createCell(1).setCellValue(cityStats.getEnterpriseoperateid());//隐藏列
                    xssfRow2.createCell(2).setCellValue(cityStats.getGasstation());
                    xssfRow2.createCell(3).setCellValue(cityStats.getPersonid());//隐藏列
                    xssfRow2.createCell(4).setCellValue(cityStats.getPersonname());
                    xssfRow2.createCell(5).setCellValue(cityStats.getPersonno());
                    xssfRow2.createCell(6).setCellValue(cityStats.getFivecardnum());//五进卡
                    xssfRow2.createCell(7).setCellValue(cityStats.getFivecardnumyx());
                    xssfRow2.createCell(8).setCellValue(cityStats.getFivecardreward());
                    xssfRow2.createCell(9).setCellValue(cityStats.getStationcardnum());//站内卡
                    xssfRow2.createCell(10).setCellValue(cityStats.getStationcardnumyx());
                    xssfRow2.createCell(11).setCellValue(cityStats.getStationcardreward());
                    xssfRow2.createCell(12).setCellValue(cityStats.getOrgcardnum());//机构卡
                    xssfRow2.createCell(13).setCellValue(cityStats.getOrgcardnumyx());
                    xssfRow2.createCell(14).setCellValue(cityStats.getQycustq3());
                    xssfRow2.createCell(15).setCellValue(cityStats.getQycustrewardq3());
                    xssfRow2.createCell(16).setCellValue(cityStats.getQycusth3());
                    xssfRow2.createCell(17).setCellValue(cityStats.getQycustrewardh3());
                    xssfRow2.createCell(18).setCellValue(cityStats.getCycustq3_gr());//个人柴油前三
                    xssfRow2.createCell(19).setCellValue(cityStats.getCycusth3_gr());//个人柴油后三
                    xssfRow2.createCell(20).setCellValue(cityStats.getCycustq3());
                    xssfRow2.createCell(21).setCellValue(cityStats.getCycustrewardq3());
                    xssfRow2.createCell(22).setCellValue(cityStats.getCycusth3());
                    xssfRow2.createCell(23).setCellValue(cityStats.getCycustrewardh3());
                    xssfRow2.createCell(24).setCellValue(cityStats.getRewardall());
                    xssfRow2.createCell(25).setCellValue(cityStats.getStarttime());
                    xssfRow2.createCell(26).setCellValue(cityStats.getEndtime());
                }
            }
            //隐藏未使用列
            xssfSheet2.setColumnHidden(0, true);
            xssfSheet2.setColumnHidden(1, true);
            xssfSheet2.setColumnHidden(3, true);

            //导入 个人办卡消费明细 Excel
            exportCallBack.onExportProcess("正在导出“个人办卡消费明细”报表(9/10)");
            //个人办卡消费明细
            // 第二步，在webbook中添加一个sheet,对应Excel文件中的sheet
            XSSFSheet xssfSheet3 = xssfWorkbook.createSheet("个人办卡消费明细");
            // 第三步，在sheet中添加表头第0行,注意老版本poi对Excel的行数列数有限制short
            XSSFRow xssfRow3 = xssfSheet3.createRow(0);
            // 第四步，创建单元格，并设置值表头 设置表头居中
            XSSFCellStyle xssfCellStyle3 = xssfWorkbook.createCellStyle();
            //居中样式
            xssfCellStyle3.setAlignment(HorizontalAlignment.LEFT);

            String[] titles3 = new String[]{"索引号", "机构ID", "地市分公司", "人员ID", "员工姓名", "员工编号", "客户姓名", "办卡类别", "卡类型", "加油卡号", "首次充值金额", "办卡时间", "手机号",
                    "汽油消费金额", "柴油消费金额","非油消费金额", "是否在分队前500"};
            XSSFCell xssfCell3 = null;
            for (int i = 0; i < titles3.length; i++) {
                xssfCell3 = xssfRow3.createCell(i);//列索引从0开始
                xssfCell3.setCellValue(titles3[i]);//列名1
                xssfCell3.setCellStyle(xssfCellStyle3);//列居中显示
            }
            if (viewTable4List != null && !viewTable4List.isEmpty()) {
                for (int i = 0; i < viewTable4List.size(); i++) {
                    xssfRow3 = xssfSheet3.createRow(i + 1);
                    ViewTable4 cityStats = viewTable4List.get(i);
                    xssfRow3.createCell(0).setCellValue(cityStats.getIndexid());//索引
                    xssfRow3.createCell(1).setCellValue(cityStats.getEnterpriseoperateid());//机构ID
                    xssfRow3.createCell(2).setCellValue(cityStats.getGasstation());
                    xssfRow3.createCell(3).setCellValue(cityStats.getPersonid());//人员ID
                    xssfRow3.createCell(4).setCellValue(cityStats.getPersonname());
                    xssfRow3.createCell(5).setCellValue(cityStats.getPersonno());
                    xssfRow3.createCell(6).setCellValue(cityStats.getCustomername());
                    xssfRow3.createCell(7).setCellValue(cityStats.getCardclass());
                    xssfRow3.createCell(8).setCellValue(cityStats.getCardtype());
                    xssfRow3.createCell(9).setCellValue(cityStats.getCardno());
                    xssfRow3.createCell(10).setCellValue(cityStats.getPaypricefirst());
                    xssfRow3.createCell(11).setCellValue(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(cityStats.getRecordtime()));
                    xssfRow3.createCell(12).setCellValue(cityStats.getCustomerphone());
                    xssfRow3.createCell(13).setCellValue(cityStats.getQycust());
                    xssfRow3.createCell(14).setCellValue(cityStats.getCycust());
                    xssfRow3.createCell(15).setCellValue(cityStats.getFycust());
                    xssfRow3.createCell(16).setCellValue(cityStats.getFlag500());
                }
            }
            //隐藏未使用列
            xssfSheet3.setColumnHidden(0, true);
            xssfSheet3.setColumnHidden(1, true);
            xssfSheet3.setColumnHidden(3, true);

            //导入异常卡到Excel
            exportCallBack.onExportProcess("正在导出“异常卡”报表(10/10)");
            //异常卡
            // 第二步，在webbook中添加一个sheet,对应Excel文件中的sheet
            XSSFSheet xssfSheet4 = xssfWorkbook.createSheet("异常卡信息");
            // 第三步，在sheet中添加表头第0行,注意老版本poi对Excel的行数列数有限制short
            XSSFRow xssfRow4 = xssfSheet4.createRow(0);
            // 第四步，创建单元格，并设置值表头 设置表头居中
            XSSFCellStyle xssfCellStyle4 = xssfWorkbook.createCellStyle();
            //居中样式
            xssfCellStyle4.setAlignment(HorizontalAlignment.LEFT);

            String[] titles4 = new String[]{"索引号", "机构ID", "地市分公司", "员工ID", "员工姓名", "员工编号", "客户姓名", "卡类别", "卡号", "手机号是否一致", "卡客户平台手机号", "石油发卡系统手机号", "证件号是否一致", "卡客户平台证件号",
                    "石油发卡系统证件号", "UK是否一致", "卡客户平台UK", "石油发卡系统UK", "其他异常"};
            XSSFCell xssfCell4 = null;
            for (int i = 0; i < titles4.length; i++) {
                xssfCell4 = xssfRow4.createCell(i);//列索引从0开始
                xssfCell4.setCellValue(titles4[i]);//列名1
                xssfCell4.setCellStyle(xssfCellStyle4);//列居中显示
            }

            if (viewTable5List != null && !viewTable5List.isEmpty()) {
                for (int i = 0; i < viewTable5List.size(); i++) {
                    xssfRow4 = xssfSheet4.createRow(i + 1);
                    ViewTable5 cityStats = viewTable5List.get(i);
                    xssfRow4.createCell(0).setCellValue(cityStats.getIndexid());//索引號
                    xssfRow4.createCell(1).setCellValue(cityStats.getEnterpriseoperateid());//机构ID
                    xssfRow4.createCell(2).setCellValue(cityStats.getGasstation());
                    xssfRow4.createCell(3).setCellValue(cityStats.getPersonid());//人员ID
                    xssfRow4.createCell(4).setCellValue(cityStats.getPersonname());
                    xssfRow4.createCell(5).setCellValue(cityStats.getPersonno());
                    xssfRow4.createCell(6).setCellValue(cityStats.getCustomername());
                    xssfRow4.createCell(7).setCellValue(cityStats.getCardclass());
                    xssfRow4.createCell(8).setCellValue(cityStats.getCardno());
                    xssfRow4.createCell(9).setCellValue(cityStats.getPhoneyz());
                    xssfRow4.createCell(10).setCellValue(cityStats.getPhonedatame());
                    xssfRow4.createCell(11).setCellValue(cityStats.getPhonedatathat());
                    xssfRow4.createCell(12).setCellValue(cityStats.getNoyz());
                    xssfRow4.createCell(13).setCellValue(cityStats.getNodatame());
                    xssfRow4.createCell(14).setCellValue(cityStats.getNodatathat());
                    xssfRow4.createCell(15).setCellValue(cityStats.getUkeyyz());
                    xssfRow4.createCell(16).setCellValue(cityStats.getUkeydatame());
                    xssfRow4.createCell(17).setCellValue(cityStats.getUkeydatathat());
                    xssfRow4.createCell(18).setCellValue(cityStats.getFlag());
                }
            }
            //隐藏未使用列
            xssfSheet4.setColumnHidden(0, true);
            xssfSheet4.setColumnHidden(1, true);
            xssfSheet4.setColumnHidden(3, true);
            //默认位置
            String fileName = "C:/" + paramYear + "年" + paramMonth + "月数据报表" + CommonUtils.createID() + ".xlsx";
            FileOutputStream out = new FileOutputStream(fileName);
            xssfWorkbook.write(out);
            out.flush();
            out.close();
            xssfWorkbook.close();
            System.out.println((System.currentTimeMillis() - s) / 1000 + "");
            exportCallBack.onExportSuccess("数据导出成功，文件路径" + fileName);
        } catch (Exception ex) {
            ex.printStackTrace();
            exportCallBack.onExportError(ex.getMessage());
        } finally {
            if (cs != null) {
                cs.close();
                cs = null;
            }
            if (cs1 != null) {
                cs1.close();
                cs1 = null;
            }
            if (cs2 != null) {
                cs2.close();
                cs2 = null;
            }
            if (cs3 != null) {
                cs3.close();
                cs3 = null;
            }
            if (cs4 != null) {
                cs4.close();
                cs4 = null;
            }
            if (cs5 != null) {
                cs5.close();
                cs5 = null;
            }
            DBUtils.closeConnection();
        }
        return null;
    }

    @Override
    protected void process(List<Integer> chunks) {

    }

    @Override
    protected void done() {
        exportCallBack.onExportComplete();
    }
}
