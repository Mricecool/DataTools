package com.datatools;

import com.datatools.bean.*;
import org.apache.poi.xssf.usermodel.*;

import javax.swing.*;
import java.io.File;
import java.io.FileInputStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class ImportSwingWorker extends SwingWorker<Integer, Integer> {

    private ExportCallBack exportCallBack;
    private String filePath;
    private Connection connection;
    private String paramYear;
    private String paramMonth;
    private String paramDate;

    public ImportSwingWorker(String filePath, String paramYear, String paramMonth, String paramDate, ExportCallBack exportCallBack) {
        this.filePath = filePath;
        this.paramYear = paramYear;
        this.paramMonth = paramMonth;
        this.paramDate = paramDate;
        this.exportCallBack = exportCallBack;
        connection = DBUtils.getConnection();
    }

    @Override
    protected Integer doInBackground() throws Exception {
        CallableStatement cs = null;
        FileInputStream isr = null;
        PreparedStatement ps1 = null;
        PreparedStatement ps2 = null;
        PreparedStatement ps3 = null;
        PreparedStatement ps4 = null;
        PreparedStatement ps5 = null;
        String sql1 = "";
        String sql2 = "";
        String sql3 = "";
        String sql4 = "";
        String sql5 = "";
        try {
            long s = System.currentTimeMillis();
            exportCallBack.onExportStart();
            exportCallBack.onExportProcess("正在检测导出环境...");
            //创建或清空数据报表
            cs = connection.prepareCall("{call proc_datatable_create(?)}");
            cs.setString(1, paramDate);
            cs.execute();

            File file = new File(filePath);
            isr = new FileInputStream(file);
            //获取工作薄
            XSSFWorkbook workbook = new XSSFWorkbook(isr);
            int sheetNums = workbook.getNumberOfSheets();
            //必须为5个sheet
            if (sheetNums != 5) {
                exportCallBack.onExportError("导入失败，Excel页签（sheet）必须为5个，请规范Excel数据格式");
                return null;
            }
            //获取第一个sheet
            XSSFSheet xssfSheet = workbook.getSheetAt(0);
            //获得总列数
            int coloumNum = xssfSheet.getRow(0).getPhysicalNumberOfCells();
            if (!"地市办卡统计".equals(xssfSheet.getSheetName()) || coloumNum != 17) {
                exportCallBack.onExportError("导入失败，请确保Excel中表1为“地市办卡统计”数据，请规范Excel数据格式");
                return null;
            }
            //获取第二个sheet
            XSSFSheet xssfSheet1 = workbook.getSheetAt(1);
            //获得总列数
            int coloumNum1 = xssfSheet1.getRow(0).getPhysicalNumberOfCells();
            if (!"地市奖励统计".equals(xssfSheet1.getSheetName()) || coloumNum1 != 18) {
                exportCallBack.onExportError("导入失败，请确保Excel中表2为“地市奖励统计”数据，请规范Excel数据格式");
                return null;
            }
            //获取第三个sheet
            XSSFSheet xssfSheet2 = workbook.getSheetAt(2);
            //获得总列数
            int coloumNum2 = xssfSheet2.getRow(0).getPhysicalNumberOfCells();
            if (!"个人办卡消费统计".equals(xssfSheet2.getSheetName()) || coloumNum2 != 27) {
                exportCallBack.onExportError("导入失败，请确保Excel中表3为“个人办卡消费统计”数据，请规范Excel数据格式");
                return null;
            }
            //获取第四个sheet
            XSSFSheet xssfSheet3 = workbook.getSheetAt(3);
            //获得总列数
            int coloumNum3 = xssfSheet3.getRow(0).getPhysicalNumberOfCells();
            if (!"个人办卡消费明细".equals(xssfSheet3.getSheetName()) || coloumNum3 != 17) {
                exportCallBack.onExportError("导入失败，请确保Excel中表4为“个人办卡消费明细”数据，请规范Excel数据格式");
                return null;
            }
            //获取第五个sheet
            XSSFSheet xssfSheet4 = workbook.getSheetAt(4);
            //获得总列数
            int coloumNum4 = xssfSheet4.getRow(0).getPhysicalNumberOfCells();
            if (!"异常卡信息".equals(xssfSheet4.getSheetName()) || coloumNum4 != 19) {
                exportCallBack.onExportError("导入失败，请确保Excel中表5为“异常卡信息”数据，请规范Excel数据格式");
                return null;
            }

            exportCallBack.onExportProcess("正在处理“地市办卡统计”报表");
            //读取sheet1中的数据
            List<ViewTable1> data1 = new ArrayList<ViewTable1>();
            ViewTable1 item1;
            // 处理当前页，循环读取每一行
            for (int rowNum = 1; rowNum <= xssfSheet.getLastRowNum(); rowNum++) {
                XSSFRow xssfRow = xssfSheet.getRow(rowNum);
                item1 = new ViewTable1();
                item1.setIndexid(Integer.parseInt(getStringVal(xssfRow.getCell(0))));
                item1.setEnterpriseoperateid(Long.parseLong(getStringVal(xssfRow.getCell(1))));
                item1.setGasstation(getStringVal(xssfRow.getCell(2)));
                item1.setActivenum(Integer.parseInt(getStringVal(xssfRow.getCell(3))));
                item1.setActivecardnum(Integer.parseInt(getStringVal(xssfRow.getCell(4))));
                item1.setFivecardnum(Integer.parseInt(getStringVal(xssfRow.getCell(5))));
                item1.setFivenewnum(Integer.parseInt(getStringVal(xssfRow.getCell(6))));
                item1.setPriceactive(Double.parseDouble(getStringVal(xssfRow.getCell(7))));
                item1.setPricefive(Double.parseDouble(getStringVal(xssfRow.getCell(8))));
                item1.setStationcardnum(Integer.parseInt(getStringVal(xssfRow.getCell(9))));
                item1.setStationnewnum(Integer.parseInt(getStringVal(xssfRow.getCell(10))));
                item1.setOrgcardnum(Integer.parseInt(getStringVal(xssfRow.getCell(11))));
                item1.setOrgnewnum(Integer.parseInt(getStringVal(xssfRow.getCell(12))));
                item1.setPricestation(Double.parseDouble(getStringVal(xssfRow.getCell(13))));
                item1.setPriceall(Double.parseDouble(getStringVal(xssfRow.getCell(14))));
                item1.setStarttime(getStringVal(xssfRow.getCell(15)));
                item1.setEndtime(getStringVal(xssfRow.getCell(16)));
                data1.add(item1);
            }
            exportCallBack.onExportProcess("正在处理“地市奖励统计”报表");
            //读取sheet2中的数据
            List<ViewTable2> data2 = new ArrayList<ViewTable2>();
            ViewTable2 item2;
            for (int rowNum = 1; rowNum <= xssfSheet1.getLastRowNum(); rowNum++) {
                XSSFRow xssfRow = xssfSheet1.getRow(rowNum);
                item2 = new ViewTable2();
                item2.setIndexid(Integer.parseInt(getStringVal(xssfRow.getCell(0))));
                item2.setEnterpriseoperateid(Long.parseLong(getStringVal(xssfRow.getCell(1))));
                item2.setGasstation(getStringVal(xssfRow.getCell(2)));
                item2.setFivecardnum(Integer.parseInt(getStringVal(xssfRow.getCell(3))));
                item2.setFivecardreward(Integer.parseInt(getStringVal(xssfRow.getCell(4))));
                item2.setStationcardnum(Integer.parseInt(getStringVal(xssfRow.getCell(5))));
                item2.setStationcardreward(Integer.parseInt(getStringVal(xssfRow.getCell(6))));
                item2.setQycust(Double.parseDouble(getStringVal(xssfRow.getCell(7))));
                item2.setQycustreward(Double.parseDouble(getStringVal(xssfRow.getCell(8))));
                item2.setQycusth3(Double.parseDouble(getStringVal(xssfRow.getCell(9))));
                item2.setQycustrewardh3(Double.parseDouble(getStringVal(xssfRow.getCell(10))));
                item2.setCycust(Double.parseDouble(getStringVal(xssfRow.getCell(11))));
                item2.setCycustreward(Double.parseDouble(getStringVal(xssfRow.getCell(12))));
                item2.setCycusth3(Double.parseDouble(getStringVal(xssfRow.getCell(13))));
                item2.setCycustrewardh3(Double.parseDouble(getStringVal(xssfRow.getCell(14))));
                item2.setRewardall(Double.parseDouble(getStringVal(xssfRow.getCell(15))));
                item2.setStarttime(getStringVal(xssfRow.getCell(16)));
                item2.setEndtime(getStringVal(xssfRow.getCell(17)));
                data2.add(item2);
            }
            exportCallBack.onExportProcess("正在处理“个人办卡消费统计”报表");
            //读取sheet3中的数据
            List<ViewTable3> data3 = new ArrayList<ViewTable3>();
            ViewTable3 item3;
            for (int rowNum = 1; rowNum <= xssfSheet2.getLastRowNum(); rowNum++) {
                XSSFRow xssfRow = xssfSheet2.getRow(rowNum);
                item3 = new ViewTable3();
                item3.setIndexid(Integer.parseInt(getStringVal(xssfRow.getCell(0))));
                item3.setEnterpriseoperateid(Long.parseLong(getStringVal(xssfRow.getCell(1))));
                item3.setGasstation(getStringVal(xssfRow.getCell(2)));
                item3.setPersonid(Integer.parseInt(getStringVal(xssfRow.getCell(3))));
                item3.setPersonname(getStringVal(xssfRow.getCell(4)));
                item3.setPersonno(getStringVal(xssfRow.getCell(5)));
                item3.setFivecardnum(Integer.parseInt(getStringVal(xssfRow.getCell(6))));
                item3.setFivecardnumyx(Integer.parseInt(getStringVal(xssfRow.getCell(7))));
                item3.setFivecardreward(Double.parseDouble(getStringVal(xssfRow.getCell(8))));
                item3.setStationcardnum(Integer.parseInt(getStringVal(xssfRow.getCell(9))));
                item3.setStationcardnumyx(Integer.parseInt(getStringVal(xssfRow.getCell(10))));
                item3.setStationcardreward(Double.parseDouble(getStringVal(xssfRow.getCell(11))));
                item3.setOrgcardnum(Integer.parseInt(getStringVal(xssfRow.getCell(12))));
                item3.setOrgcardnumyx(Integer.parseInt(getStringVal(xssfRow.getCell(13))));
                item3.setQycustq3(Double.parseDouble(getStringVal(xssfRow.getCell(14))));
                item3.setQycustrewardq3(Double.parseDouble(getStringVal(xssfRow.getCell(15))));
                item3.setQycusth3(Double.parseDouble(getStringVal(xssfRow.getCell(16))));
                item3.setQycustrewardh3(Double.parseDouble(getStringVal(xssfRow.getCell(17))));
                item3.setCycustq3_gr(Double.parseDouble(getStringVal(xssfRow.getCell(18))));
                item3.setCycusth3_gr(Double.parseDouble(getStringVal(xssfRow.getCell(19))));
                item3.setCycustq3(Double.parseDouble(getStringVal(xssfRow.getCell(20))));
                item3.setCycustrewardq3(Double.parseDouble(getStringVal(xssfRow.getCell(21))));
                item3.setCycusth3(Double.parseDouble(getStringVal(xssfRow.getCell(22))));
                item3.setCycustrewardh3(Double.parseDouble(getStringVal(xssfRow.getCell(23))));
                item3.setRewardall(Double.parseDouble(getStringVal(xssfRow.getCell(24))));
                item3.setStarttime(getStringVal(xssfRow.getCell(25)));
                item3.setEndtime(getStringVal(xssfRow.getCell(26)));
                data3.add(item3);
            }
            exportCallBack.onExportProcess("正在处理“个人办卡消费明细”报表");
            //读取sheet4中的数据
            List<ViewTable4> data4 = new ArrayList<ViewTable4>();
            ViewTable4 item4;
            for (int rowNum = 1; rowNum <= xssfSheet3.getLastRowNum(); rowNum++) {
                XSSFRow xssfRow = xssfSheet3.getRow(rowNum);
                item4 = new ViewTable4();
                item4.setIndexid(Integer.parseInt(getStringVal(xssfRow.getCell(0))));
                item4.setEnterpriseoperateid(Long.parseLong(getStringVal(xssfRow.getCell(1))));
                item4.setGasstation(getStringVal(xssfRow.getCell(2)));
                item4.setPersonid(Long.parseLong(getStringVal(xssfRow.getCell(3))));
                item4.setPersonname(getStringVal(xssfRow.getCell(4)));
                item4.setPersonno(getStringVal(xssfRow.getCell(5)));
                item4.setCustomername(getStringVal(xssfRow.getCell(6)));
                item4.setCardclass(getStringVal(xssfRow.getCell(7)));
                item4.setCardtype(getStringVal(xssfRow.getCell(8)));
                item4.setCardno(getStringVal(xssfRow.getCell(9)));
                item4.setPaypricefirst(Double.parseDouble(getStringVal(xssfRow.getCell(10))));
                item4.setRecordtime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(getStringVal(xssfRow.getCell(11))));
                item4.setCustomerphone(getStringVal(xssfRow.getCell(12)));
                item4.setQycust(Double.parseDouble(getStringVal(xssfRow.getCell(13))));
                item4.setCycust(Double.parseDouble(getStringVal(xssfRow.getCell(14))));
                item4.setFycust(Double.parseDouble(getStringVal(xssfRow.getCell(15))));
                item4.setFlag500(getStringVal(xssfRow.getCell(16)));
                data4.add(item4);
            }
            exportCallBack.onExportProcess("正在处理“异常卡信息”报表");
            //读取sheet5中的数据
            List<ViewTable5> data5 = new ArrayList<ViewTable5>();
            ViewTable5 item5;
            for (int rowNum = 1; rowNum <= xssfSheet4.getLastRowNum(); rowNum++) {
                XSSFRow xssfRow = xssfSheet4.getRow(rowNum);
                item5 = new ViewTable5();
                item5.setIndexid(Integer.parseInt(getStringVal(xssfRow.getCell(0))));
                item5.setEnterpriseoperateid(Long.parseLong(getStringVal(xssfRow.getCell(1))));
                item5.setGasstation(getStringVal(xssfRow.getCell(2)));
                item5.setPersonid(Long.parseLong(getStringVal(xssfRow.getCell(3))));
                item5.setPersonname(getStringVal(xssfRow.getCell(4)));
                item5.setPersonno(getStringVal(xssfRow.getCell(5)));
                item5.setCustomername(getStringVal(xssfRow.getCell(6)));
                item5.setCardclass(getStringVal(xssfRow.getCell(7)));
                item5.setCardno(getStringVal(xssfRow.getCell(8)));
                item5.setPhoneyz(getStringVal(xssfRow.getCell(9)));
                item5.setPhonedatame(getStringVal(xssfRow.getCell(10)));
                item5.setPhonedatathat(getStringVal(xssfRow.getCell(11)));
                item5.setNoyz(getStringVal(xssfRow.getCell(12)));
                item5.setNodatame(getStringVal(xssfRow.getCell(13)));
                item5.setNodatathat(getStringVal(xssfRow.getCell(14)));
                item5.setUkeyyz(getStringVal(xssfRow.getCell(15)));
                item5.setUkeydatame(getStringVal(xssfRow.getCell(16)));
                item5.setUkeydatathat(getStringVal(xssfRow.getCell(17)));
                item5.setFlag(getStringVal(xssfRow.getCell(18)));
                data5.add(item5);
            }
            exportCallBack.onExportProcess("正在将数据导入到数据库");
            //将表数据存入到数据库
            connection.setAutoCommit(false);
            sql1 = "INSERT INTO DataTable1_" + paramDate + " (IndexID,EnterpriseOperateID,GasStation,ActiveNum,ActiveCardNum,FiveCardNum,FiveNewNum,PriceActive,PriceFive,StationCardNum,StationNewNum,OrgCardNum,OrgNewNum,PriceStation,PriceAll,StartTime,EndTime)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            ps1 = connection.prepareStatement(sql1);
            for (ViewTable1 table1 : data1) {
                ps1.setInt(1, table1.getIndexid());
                ps1.setLong(2, table1.getEnterpriseoperateid());
                ps1.setString(3, table1.getGasstation());
                ps1.setInt(4, table1.getActivenum());
                ps1.setInt(5, table1.getActivecardnum());
                ps1.setInt(6, table1.getFivecardnum());
                ps1.setInt(7, table1.getFivenewnum());
                ps1.setDouble(8, table1.getPriceactive());
                ps1.setDouble(9, table1.getPricefive());
                ps1.setInt(10, table1.getStationcardnum());
                ps1.setInt(11, table1.getStationnewnum());
                ps1.setInt(12, table1.getOrgcardnum());
                ps1.setInt(13, table1.getOrgnewnum());
                ps1.setDouble(14, table1.getPricestation());
                ps1.setDouble(15, table1.getPriceall());
                ps1.setString(16, table1.getStarttime());
                ps1.setString(17, table1.getEndtime());
                ps1.addBatch();
            }
            ps1.executeBatch();
            connection.commit();

            sql2 = "INSERT INTO DataTable2_" + paramDate + " (IndexID,EnterpriseOperateID,GasStation,FiveCardNum,FiveCardReward,StationCardNum,StationCardReward,QyCust,QyCustReward,QyCustH3,QyCustRewardH3,CyCust,CyCustReward,CyCustH3,CyCustRewardH3,RewardAll,StartTime,EndTime)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            ps2 = connection.prepareStatement(sql2);
            for (ViewTable2 table2 : data2) {
                ps2.setInt(1, table2.getIndexid());
                ps2.setLong(2, table2.getEnterpriseoperateid());
                ps2.setString(3, table2.getGasstation());
                ps2.setInt(4, table2.getFivecardnum());
                ps2.setInt(5, table2.getFivecardreward());
                ps2.setInt(6, table2.getStationcardnum());
                ps2.setInt(7, table2.getStationcardreward());
                ps2.setDouble(8, table2.getQycust());
                ps2.setDouble(9, table2.getQycustreward());
                ps2.setDouble(10, table2.getQycusth3());
                ps2.setDouble(11, table2.getQycustrewardh3());
                ps2.setDouble(12, table2.getCycust());
                ps2.setDouble(13, table2.getCycustreward());
                ps2.setDouble(14, table2.getCycusth3());
                ps2.setDouble(15, table2.getCycustrewardh3());
                ps2.setDouble(16, table2.getRewardall());
                ps2.setString(17, table2.getStarttime());
                ps2.setString(18, table2.getEndtime());
                ps2.addBatch();
            }
            ps2.executeBatch();
            connection.commit();

            sql3 = "INSERT INTO DataTable3_" + paramDate + " (IndexID,EnterpriseOperateID,GasStation,PersonID,PersonName,PersonNo,FiveCardNum,FiveCardNumYX,FiveCardReward,StationCardNum,StationCardNumYX,StationCardReward,OrgCardNum,OrgCardNumYX,QyCustQ3,QyCustRewardQ3,QyCustH3,QyCustRewardH3,CyCustQ3_GR,CyCustH3_GR,CyCustQ3,CyCustRewardQ3,CyCustH3,CyCustRewardH3,RewardAll,StartTime,EndTime)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            ps3 = connection.prepareStatement(sql3);
            for (ViewTable3 table3 : data3) {
                ps3.setLong(1, table3.getIndexid());
                ps3.setLong(2, table3.getEnterpriseoperateid());
                ps3.setString(3, table3.getGasstation());
                ps3.setInt(4, table3.getPersonid());
                ps3.setString(5, table3.getPersonname());
                ps3.setString(6, table3.getPersonno());
                ps3.setInt(7, table3.getFivecardnum());
                ps3.setInt(8, table3.getFivecardnumyx());
                ps3.setDouble(9, table3.getFivecardreward());
                ps3.setInt(10, table3.getStationcardnum());
                ps3.setInt(11, table3.getStationcardnumyx());
                ps3.setDouble(12, table3.getStationcardreward());
                ps3.setInt(13, table3.getOrgcardnum());
                ps3.setInt(14, table3.getOrgcardnumyx());
                ps3.setDouble(15, table3.getQycustq3());
                ps3.setDouble(16, table3.getQycustrewardq3());
                ps3.setDouble(17, table3.getQycusth3());
                ps3.setDouble(18, table3.getQycustrewardh3());
                ps3.setDouble(19, table3.getCycustq3_gr());
                ps3.setDouble(20, table3.getCycusth3_gr());
                ps3.setDouble(21, table3.getCycustq3());
                ps3.setDouble(22, table3.getCycustrewardq3());
                ps3.setDouble(23, table3.getCycusth3());
                ps3.setDouble(24, table3.getCycustrewardh3());
                ps3.setDouble(25, table3.getRewardall());
                ps3.setString(26, table3.getStarttime());
                ps3.setString(27, table3.getEndtime());
                ps3.addBatch();
            }
            ps3.executeBatch();
            connection.commit();


            sql4 = "INSERT INTO DataTable4_" + paramDate + " (IndexID,EnterpriseOperateID,GasStation,PersonID,PersonName,PersonNo,CustomerName,CardClass,CardType,CardNo,PayPriceFirst,RecordTime,CustomerPhone,QyCust,CyCust,FyCust,flag500)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            ps4 = connection.prepareStatement(sql4);
            for (ViewTable4 table4 : data4) {
                ps4.setInt(1, table4.getIndexid());
                ps4.setLong(2, table4.getEnterpriseoperateid());
                ps4.setString(3, table4.getGasstation());
                ps4.setLong(4, table4.getPersonid());
                ps4.setString(5, table4.getPersonname());
                ps4.setString(6, table4.getPersonno());
                ps4.setString(7, table4.getCustomername());
                ps4.setString(8, table4.getCardclass());
                ps4.setString(9, table4.getCardtype());
                ps4.setString(10, table4.getCardno());
                ps4.setDouble(11, table4.getPaypricefirst());
                ps4.setString(12, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(table4.getRecordtime()));
                ps4.setString(13, table4.getCustomerphone());
                ps4.setDouble(14, table4.getQycust());
                ps4.setDouble(15, table4.getCycust());
                ps4.setDouble(16, table4.getFycust());
                ps4.setString(17, table4.getFlag500());
                ps4.addBatch();
            }
            ps4.executeBatch();
            connection.commit();

            sql5 = "INSERT INTO DataTable5_" + paramDate + " (IndexID,EnterpriseOperateID,GasStation,PersonID,PersonName,PersonNo,CustomerName,CardClass,CardNo,PhoneYZ,PhoneDataMe,PhoneDataThat,NoYZ,NoDataMe,NoDataThat,UkeyYZ,UkeyDataMe,UkeyDataThat,Flag)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            ps5 = connection.prepareStatement(sql5);
            for (ViewTable5 table5 : data5) {
                ps5.setInt(1, table5.getIndexid());
                ps5.setLong(2, table5.getEnterpriseoperateid());
                ps5.setString(3, table5.getGasstation());
                ps5.setLong(4, table5.getPersonid());
                ps5.setString(5, table5.getPersonname());
                ps5.setString(6, table5.getPersonno());
                ps5.setString(7, table5.getCustomername());
                ps5.setString(8, table5.getCardclass());
                ps5.setString(9, table5.getCardno());
                ps5.setString(10, table5.getPhoneyz());
                ps5.setString(11, table5.getPhonedatame());
                ps5.setString(12, table5.getPhonedatathat());
                ps5.setString(13, table5.getNoyz());
                ps5.setString(14, table5.getNodatame());
                ps5.setString(15, table5.getNodatathat());
                ps5.setString(16, table5.getUkeyyz());
                ps5.setString(17, table5.getUkeydatame());
                ps5.setString(18, table5.getUkeydatathat());
                ps5.setString(19, table5.getFlag());
                ps5.addBatch();
            }
            ps5.executeBatch();
            connection.commit();
            exportCallBack.onExportSuccess(paramYear + "年" + paramMonth + "月数据报表导入到数据库成功");
            System.out.println((System.currentTimeMillis() - s) / 1000 + "");
        } catch (ParseException ex) {
            ex.printStackTrace();
            exportCallBack.onExportError(ex.getMessage() + "，请仔细核对数据格式");
        } catch (Exception ex) {
            ex.printStackTrace();
            exportCallBack.onExportError(ex.getMessage() + "，请仔细核对数据");
        } finally {
            if (cs != null) {
                cs.close();
                cs = null;
            }
            if (isr != null) {
                isr.close();
                isr = null;
            }
            if (ps1 != null) {
                ps1.close();
                ps1 = null;
            }
            if (ps2 != null) {
                ps2.close();
                ps2 = null;
            }
            if (ps3 != null) {
                ps3.close();
                ps3 = null;
            }
            if (ps4 != null) {
                ps4.close();
                ps4 = null;
            }
            if (ps5 != null) {
                ps5.close();
                ps5 = null;
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

    public static String getStringVal(XSSFCell cell) {
        if (cell != null && null != cell.getCellTypeEnum()) {
            switch (cell.getCellTypeEnum()) {
                case NUMERIC:
                    long longVal = Math.round(cell.getNumericCellValue());
                    double doubleVal = cell.getNumericCellValue();
                    if (Double.parseDouble(longVal + ".0") == doubleVal) {
                        return longVal + "";
                    } else {
                        return doubleVal + "";
                    }
                case STRING:
                    return cell.getStringCellValue();
                default:
                    return "";
            }
        } else {
            return "";
        }
    }
}
