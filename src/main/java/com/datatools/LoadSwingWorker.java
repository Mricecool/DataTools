package com.datatools;

import com.csvreader.CsvReader;

import javax.swing.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.List;

/**
 * 异步基础数据导入
 */
public class LoadSwingWorker extends SwingWorker<Integer, Integer> {

    //文件名
    private String filePath;
    //操作类型
    private Integer operType;
    //表日期
    private String tableName;
    //回调
    private SwingWorkerCallBack swingWorkerCallBack;
    private Connection connection;

    public LoadSwingWorker(String filePath, Integer operType, String tableName, SwingWorkerCallBack swingWorkerCallBack) {
        this.filePath = filePath;
        this.operType = operType;
        this.tableName = tableName;
        this.swingWorkerCallBack = swingWorkerCallBack;
        connection = DBUtils.getConnection();
    }

    @Override
    protected Integer doInBackground() throws Exception {
        InputStreamReader isr = null;
        CsvReader csvReader = null;
        PreparedStatement pstmt = null;
        CallableStatement cs = null;
        String sql = null;
        PreparedStatement pstmt_del = null;
        String sql_del = null;
        try {
            long s = System.currentTimeMillis();
            File file = new File(filePath);
            isr = new InputStreamReader(new FileInputStream(file), "UTF-8");
            // 创建CSV读对象
            csvReader = new CsvReader(isr);
            // 读表头
            csvReader.readHeaders();
            connection.setAutoCommit(false);
            switch (operType) {
                case 1:  //个人卡片，数据库设置唯一约束，无法清除源数据，直接插入
                    if (csvReader.getHeaderCount() == 14) {
                        swingWorkerCallBack.onStart();
                        sql = "INSERT INTO PersonCard(ID,CAUNIQUEID,CURRENTASN,ONAME,O1NAME,RECORDTIME,CANAME,DITTEXT,CUIDNO,MOBILEPHONE,GASSTATIONNO,UKey)VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
                        pstmt = connection.prepareStatement(sql);
                        while (csvReader.readRecord()) {
                            // 读一整行
                            //System.out.println(csvReader.getRawRecord());
                            pstmt.setString(1, csvReader.get(0));
                            pstmt.setString(2, csvReader.get("CAUNIQUEID"));
                            pstmt.setString(3, csvReader.get("CURRENTASN"));
                            pstmt.setString(4, csvReader.get("ONAME"));
                            pstmt.setString(5, csvReader.get("O1NAME"));
                            pstmt.setString(6, csvReader.get("RECORDTIME"));
                            pstmt.setString(7, csvReader.get("CANAME"));
                            pstmt.setString(8, csvReader.get("DITTEXT"));
                            pstmt.setString(9, csvReader.get("CUIDNO"));
                            pstmt.setString(10, csvReader.get("MOBILEPHONE"));
                            pstmt.setString(11, csvReader.get("OUNIQUEID"));
                            pstmt.setString(12, csvReader.get("OPUNIQUEID"));
                            pstmt.addBatch();
                            // 1w条记录插入一次
                            if (csvReader.getCurrentRecord() % 10000 == 0) {
                                //不再分批提交，保证事务一次性
                                /*pstmt.executeBatch();
                                connection.commit();*/
                                publish((int) csvReader.getCurrentRecord());
                            }
                        }
                        pstmt.executeBatch();
                        connection.commit();
                        swingWorkerCallBack.onSuccess();
                    } else {
                        swingWorkerCallBack.onFailed("导入数据失败，请检查数据格式是否正确");
                    }
                    break;
                case 2: //个人充值
                    if (csvReader.getHeaderCount() == 17) {
                        swingWorkerCallBack.onStart();
                        //如果当前表不存在，则创建
                        cs = connection.prepareCall("{call proc_table_create(?,?)}");
                        cs.setInt(1, 2);
                        cs.setString(2, tableName);
                        cs.execute();
                        //清除表数据
                        sql_del = "DELETE FROM PersonPay" + tableName;
                        pstmt_del = connection.prepareStatement(sql_del);
                        pstmt_del.executeUpdate();
                        sql = "INSERT INTO PersonPay" + tableName + " (ID,OCCURTIME,BUSINESSDATE,ONAME,O6NAME,O2NAME,CARDASN,CANAME,RECORDTIME,O5NAME,O3NAME,O4NAME,CONAME,CONTRACTEXPIREDDATE,DTTTEXT,REALAMOUNT,DTSTEXT)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
                        pstmt = connection.prepareStatement(sql);
                        while (csvReader.readRecord()) {
                            pstmt.setString(1, csvReader.get(0));
                            pstmt.setString(2, csvReader.get("OCCURTIME"));
                            pstmt.setString(3, csvReader.get("BUSINESSDATE"));
                            pstmt.setString(4, csvReader.get("ONAME"));
                            pstmt.setString(5, csvReader.get("O6NAME"));
                            pstmt.setString(6, csvReader.get("O2NAME"));
                            pstmt.setString(7, csvReader.get("CARDASN"));
                            pstmt.setString(8, csvReader.get("CANAME"));
                            pstmt.setString(9, csvReader.get("RECORDTIME"));
                            pstmt.setString(10, csvReader.get("O5NAME"));
                            pstmt.setString(11, csvReader.get("O3NAME"));
                            pstmt.setString(12, csvReader.get("O4NAME"));
                            pstmt.setString(13, csvReader.get("CONAME"));
                            pstmt.setString(14, csvReader.get("CONTRACTEXPIREDDATE"));
                            pstmt.setString(15, csvReader.get("DTTTEXT"));
                            pstmt.setString(16, csvReader.get("REALAMOUNT"));
                            pstmt.setString(17, csvReader.get("DTSTEXT"));
                            pstmt.addBatch();
                            // 1w条记录插入一次
                            if (csvReader.getCurrentRecord() % 10000 == 0) {
                                pstmt.executeBatch();
                                connection.commit();
                                publish((int) csvReader.getCurrentRecord());
                            }
                        }
                        pstmt.executeBatch();
                        connection.commit();
                        swingWorkerCallBack.onSuccess();
                    } else {
                        swingWorkerCallBack.onFailed("导入数据失败，请检查数据格式是否正确");
                    }
                    break;
                case 3: //个人消费
                    if (csvReader.getHeaderCount() == 20) {
                        swingWorkerCallBack.onStart();
                        //表不存在则创建表
                        cs = connection.prepareCall("{call proc_table_create(?,?)}");
                        cs.setInt(1, 1);
                        cs.setString(2, tableName);
                        cs.execute();
                        //清除表数据
                        sql_del = "DELETE FROM PersonCardCust" + tableName;
                        pstmt_del = connection.prepareStatement(sql_del);
                        pstmt_del.executeUpdate();
                        sql = "INSERT INTO PersonCardCust" + tableName + " (ID,OCCURTIME,BUSINESSDATE,ONAME,O6NAME,O2NAME,CARDASN,CANAME,RECORDTIME,O5NAME,O3NAME,O4NAME,CONAME,CONTRACTEXPIREDDATE,CATNAME,VOLUMN,REALAMOUNT,DISCOUNT,CONNAME,DTSTEXT)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
                        pstmt = connection.prepareStatement(sql);
                        while (csvReader.readRecord()) {
                            pstmt.setString(1, csvReader.get(0));
                            pstmt.setString(2, csvReader.get("OCCURTIME"));
                            pstmt.setString(3, csvReader.get("BUSINESSDATE"));
                            pstmt.setString(4, csvReader.get("ONAME"));
                            pstmt.setString(5, csvReader.get("O6NAME"));
                            pstmt.setString(6, csvReader.get("O2NAME"));
                            pstmt.setString(7, csvReader.get("CARDASN"));
                            pstmt.setString(8, csvReader.get("CANAME"));
                            pstmt.setString(9, csvReader.get("RECORDTIME"));
                            pstmt.setString(10, csvReader.get("O5NAME"));
                            pstmt.setString(11, csvReader.get("O3NAME"));
                            pstmt.setString(12, csvReader.get("O4NAME"));
                            pstmt.setString(13, csvReader.get("CONAME"));
                            pstmt.setString(14, csvReader.get("CONTRACTEXPIREDDATE"));
                            pstmt.setString(15, csvReader.get("CATNAME"));
                            pstmt.setString(16, csvReader.get("VOLUMN"));
                            pstmt.setString(17, csvReader.get("REALAMOUNT"));
                            pstmt.setString(18, csvReader.get("DISCOUNT"));
                            pstmt.setString(19, csvReader.get("CONNAME"));
                            pstmt.setString(20, csvReader.get("DTSTEXT"));
                            pstmt.addBatch();
                            // 1w条记录插入一次
                            if (csvReader.getCurrentRecord() % 10000 == 0) {
                                pstmt.executeBatch();
                                connection.commit();
                                publish((int) csvReader.getCurrentRecord());
                            }
                        }
                        pstmt.executeBatch();
                        connection.commit();
                        swingWorkerCallBack.onSuccess();
                    } else {
                        swingWorkerCallBack.onFailed("导入数据失败，请检查数据格式是否正确");
                    }
                    break;
                case 4: //车队卡片
                    if (csvReader.getHeaderCount() == 11) {
                        swingWorkerCallBack.onStart();
                        //设置唯一约束，直接插入
                        sql = "INSERT INTO CarCard(ID,CAUNIQUEID,CANAME,ONAME,O1NAME,RECORDTIME,DITTEXT,COMIDNO,LINKMANNAME,LINKMANPHONE,GASSTATIONNO)VALUES(?,?,?,?,?,?,?,?,?,?,?)";
                        pstmt = connection.prepareStatement(sql);
                        while (csvReader.readRecord()) {
                            pstmt.setString(1, csvReader.get(0));
                            pstmt.setString(2, csvReader.get("CAUNIQUEID"));
                            pstmt.setString(3, csvReader.get("CANAME"));
                            pstmt.setString(4, csvReader.get("ONAME"));
                            pstmt.setString(5, csvReader.get("O1NAME"));
                            pstmt.setString(6, csvReader.get("RECORDTIME"));
                            pstmt.setString(7, csvReader.get("DITTEXT"));
                            pstmt.setString(8, csvReader.get("COMIDNO"));
                            pstmt.setString(9, csvReader.get("LINKMANNAME"));
                            pstmt.setString(10, csvReader.get("LINKMANPHONE"));
                            pstmt.setString(11, csvReader.get("O1UNIQUEID"));
                            pstmt.addBatch();
                            // 1w条记录插入一次
                            if (csvReader.getCurrentRecord() % 10000 == 0) {
                                //不再分批提交，保证事务，数据量很小
                                /*pstmt.executeBatch();
                                connection.commit();*/
                                publish((int) csvReader.getCurrentRecord());
                            }
                        }
                        pstmt.executeBatch();
                        connection.commit();
                        swingWorkerCallBack.onSuccess();
                    } else {
                        swingWorkerCallBack.onFailed("导入数据失败，请检查数据格式是否正确");
                    }
                    break;
                case 5: //车队充值
                    if (csvReader.getHeaderCount() == 18) {
                        swingWorkerCallBack.onStart();
                        //创建表
                        cs = connection.prepareCall("{call proc_table_create(?,?)}");
                        cs.setInt(1, 4);
                        cs.setString(2, tableName);
                        cs.execute();
                        //清除表数据
                        sql_del = "DELETE FROM CarPay" + tableName;
                        pstmt_del = connection.prepareStatement(sql_del);
                        pstmt_del.executeUpdate();
                        sql = "INSERT INTO CarPay" + tableName + " (ID,OCCURTIME,BUSINESSDATE,ONAME,O6NAME,O2NAME,CARDASN,CUUNIQUEID,CUNAME,RECORDTIME,O5NAME,O3NAME,O4NAME,CONAME,CONTRACTEXPIREDDATE,DTTTEXT,REALAMOUNT,DTSTEXT)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
                        pstmt = connection.prepareStatement(sql);
                        while (csvReader.readRecord()) {
                            pstmt.setString(1, csvReader.get(0));
                            pstmt.setString(2, csvReader.get("OCCURTIME"));
                            pstmt.setString(3, csvReader.get("BUSINESSDATE"));
                            pstmt.setString(4, csvReader.get("ONAME"));
                            pstmt.setString(5, csvReader.get("O6NAME"));
                            pstmt.setString(6, csvReader.get("O2NAME"));
                            pstmt.setString(7, csvReader.get("CARDASN"));
                            pstmt.setString(8, csvReader.get("CUUNIQUEID"));
                            pstmt.setString(9, csvReader.get("CUNAME"));
                            pstmt.setString(10, csvReader.get("RECORDTIME"));
                            pstmt.setString(11, csvReader.get("O5NAME"));
                            pstmt.setString(12, csvReader.get("O3NAME"));
                            pstmt.setString(13, csvReader.get("O4NAME"));
                            pstmt.setString(14, csvReader.get("CONAME"));
                            pstmt.setString(15, csvReader.get("CONTRACTEXPIREDDATE"));
                            pstmt.setString(16, csvReader.get("DTTTEXT"));
                            pstmt.setString(17, csvReader.get("REALAMOUNT"));
                            pstmt.setString(18, csvReader.get("DTSTEXT"));
                            pstmt.addBatch();
                            // 1w条记录插入一次
                            if (csvReader.getCurrentRecord() % 10000 == 0) {
                                pstmt.executeBatch();
                                connection.commit();
                                publish((int) csvReader.getCurrentRecord());
                            }
                        }
                        pstmt.executeBatch();
                        connection.commit();
                        swingWorkerCallBack.onSuccess();
                    } else {
                        swingWorkerCallBack.onFailed("导入数据失败，请检查数据格式是否正确");
                    }
                    break;
                case 6://车队消费
                    if (csvReader.getHeaderCount() == 21) {
                        swingWorkerCallBack.onStart();
                        //创建表
                        cs = connection.prepareCall("{call proc_table_create(?,?)}");
                        cs.setInt(1, 3);
                        cs.setString(2, tableName);
                        cs.execute();
                        //清除表数据
                        sql_del = "DELETE FROM CarCardCust" + tableName;
                        pstmt_del = connection.prepareStatement(sql_del);
                        pstmt_del.executeUpdate();
                        sql = "INSERT INTO CarCardCust" + tableName + " (ID,OCCURTIME,BUSINESSDATE,ONAME,O6NAME,O2NAME,CARDASN,CUUNIQUEID,CUNAME,RECORDTIME,O5NAME,O3NAME,O4NAME,CONAME,CONTRACTEXPIREDDATE,CATNAME,VOLUMN,REALAMOUNT,DISCOUNT,CONNAME,DTSTEXT)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
                        pstmt = connection.prepareStatement(sql);
                        while (csvReader.readRecord()) {
                            pstmt.setString(1, csvReader.get(0));
                            pstmt.setString(2, csvReader.get("OCCURTIME"));
                            pstmt.setString(3, csvReader.get("BUSINESSDATE"));
                            pstmt.setString(4, csvReader.get("ONAME"));
                            pstmt.setString(5, csvReader.get("O6NAME"));
                            pstmt.setString(6, csvReader.get("O2NAME"));
                            pstmt.setString(7, csvReader.get("CARDASN"));
                            pstmt.setString(8, csvReader.get("CUUNIQUEID"));
                            pstmt.setString(9, csvReader.get("CUNAME"));
                            pstmt.setString(10, csvReader.get("RECORDTIME"));
                            pstmt.setString(11, csvReader.get("O5NAME"));
                            pstmt.setString(12, csvReader.get("O3NAME"));
                            pstmt.setString(13, csvReader.get("O4NAME"));
                            pstmt.setString(14, csvReader.get("CONAME"));
                            pstmt.setString(15, csvReader.get("CONTRACTEXPIREDDATE"));
                            pstmt.setString(16, csvReader.get("CATNAME"));
                            pstmt.setString(17, csvReader.get("VOLUMN"));
                            pstmt.setString(18, csvReader.get("REALAMOUNT"));
                            pstmt.setString(19, csvReader.get("DISCOUNT"));
                            pstmt.setString(20, csvReader.get("CONNAME"));
                            pstmt.setString(21, csvReader.get("DTSTEXT"));
                            pstmt.addBatch();
                            // 1w条记录插入一次
                            if (csvReader.getCurrentRecord() % 10000 == 0) {
                                pstmt.executeBatch();
                                connection.commit();
                                publish((int) csvReader.getCurrentRecord());
                            }
                        }
                        pstmt.executeBatch();
                        connection.commit();
                        swingWorkerCallBack.onSuccess();
                    } else {
                        swingWorkerCallBack.onFailed("导入数据失败，请检查数据格式是否正确");
                    }
                    break;
            }
            System.out.println((System.currentTimeMillis() - s) / 1000 + "");
        } catch (IOException ex) {
            //回滚
            try {
                connection.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
                swingWorkerCallBack.onFailed(e1.getMessage());
            }
            ex.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();
            swingWorkerCallBack.onFailed(ex.getMessage());
        } finally {
            if (isr != null) {
                isr.close();
                isr = null;
            }
            if (csvReader != null) {
                csvReader.close();
                csvReader = null;
            }
            if (pstmt != null) {
                pstmt.close();
                pstmt = null;
            }
            if (pstmt_del != null) {
                pstmt_del.close();
                pstmt_del = null;
            }
            if (cs != null) {
                cs.close();
                cs = null;
            }
            DBUtils.closeConnection();
        }
        return null;
    }

    @Override
    protected void process(List<Integer> chunks) {
        int x = chunks.get(0);
        swingWorkerCallBack.onProcess(x);
    }

    @Override
    protected void done() {
        swingWorkerCallBack.onComplete();
    }
}
