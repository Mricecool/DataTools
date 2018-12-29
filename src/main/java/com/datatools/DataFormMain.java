package com.datatools;

import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.io.File;

/**
 * 数据导入导出工具主页面
 */
public class DataFormMain implements ActionListener, SwingWorkerCallBack, ExportCallBack {
    private JPanel panel1;
    //等待框
    private LoadingPanel glasspane;
    private JPanel layout_menu;
    private JPanel layout_middle;
    private JPanel layout_bottom;
    //个人卡信息
    private JButton btnPersonCard;
    //个人充值信息
    private JButton btnPersonPay;
    //个人消费信息
    private JButton btnPersonCust;
    //车队卡信息
    private JButton btnCarCard;
    //车队充值信息
    private JButton btnCarPay;
    //车队消费信息
    private JButton btnCarCust;
    //文件名
    private JTextField txtFileName;
    //选择文件
    private JButton btnSelect;
    //年
    private JComboBox cbYear;
    //月
    private JComboBox cbMonth;
    //导出
    private JButton btnExport;
    //导入
    private JButton btnImport;
    private JLabel lableFileType;
    //csv导入(导入基本数据)
    private JRadioButton csvRadioButton;
    //xlsx(将数据导入到数据库)
    private JRadioButton xlsxRadioButton;
    //导出数据报表
    private JRadioButton exportRadioButton;
    //文件选择
    private JFileChooser jf;
    //文件路径
    private static String filePath = "";
    //个人车队导入工具异步
    private LoadSwingWorker loadSwingWorker;
    //报表导出工具异步
    private ExportSwingWorker exportSwingWorker;
    //报表导入工具异步
    private ImportSwingWorker importSwingWorker;


    public DataFormMain() {
        JFrame frame = new JFrame("数据导入导出工具");
        glasspane = new LoadingPanel();
        glasspane.setBounds(600, 400, 600, 300);
        frame.setResizable(false);
        frame.setContentPane(panel1);
        frame.setGlassPane(glasspane);
        glasspane.setText("正在导入数据, 请稍后 ...");
        //frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
        frame.pack();
        frame.setSize(700, 500);
        frame.setIconImage(Toolkit.getDefaultToolkit().getImage(DataFormMain.class.getResource("/image/logo.png")));
        frame.setLocationRelativeTo(panel1);//居中
        frame.setVisible(true);


        frame.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent e) {
                if(glasspane.isVisible()){
                    return;
                }
                int a = JOptionPane.showConfirmDialog(null, "确定要关闭吗？", "温馨提示",
                        JOptionPane.YES_NO_OPTION);
                if (a == 0) {
                    System.exit(0); //关闭
                }
            }
        });

        //单选按钮组
        ButtonGroup group = new ButtonGroup();
        group.add(csvRadioButton);
        group.add(xlsxRadioButton);
        group.add(exportRadioButton);
        csvRadioButton.setSelected(true);
        //默认导出报表和导入数据库不可用
        btnExport.setEnabled(false);
        btnImport.setEnabled(false);
        //基本数据导入
        csvRadioButton.addItemListener(new ItemListener() {
            @Override
            public void itemStateChanged(ItemEvent e) {
                btnExport.setEnabled(false);
                btnImport.setEnabled(false);
                //文件选择可用
                btnSelect.setEnabled(true);
            }
        });
        //数据导入到数据库
        xlsxRadioButton.addItemListener(new ItemListener() {
            @Override
            public void itemStateChanged(ItemEvent e) {
                hideButton(0);
                btnImport.setEnabled(true);
                btnExport.setEnabled(false);
                btnSelect.setEnabled(true);
            }
        });
        //导出报表到本地
        exportRadioButton.addItemListener(new ItemListener() {
            @Override
            public void itemStateChanged(ItemEvent e) {
                hideButton(0);
                btnExport.setEnabled(true);
                btnImport.setEnabled(false);
                btnSelect.setEnabled(false);
            }
        });

        txtFileName.setPreferredSize(new Dimension(200, 50));
        //个人卡
        btnPersonCard.setActionCommand("btnPersonCard");
        //个人充值
        btnPersonPay.setActionCommand("btnPersonPay");
        //个人消费
        btnPersonCust.setActionCommand("btnPersonCust");
        //车队卡
        btnCarCard.setActionCommand("btnCarCard");
        //车队充值
        btnCarPay.setActionCommand("btnCarPay");
        //车队消费
        btnCarCust.setActionCommand("btnCarCust");
        //文件选择
        btnSelect.setActionCommand("btnSelect");
        //文件导出
        btnExport.setActionCommand("btnExport");
        //文件导入
        btnImport.setActionCommand("btnImport");
        btnPersonCard.addActionListener(this);
        btnPersonPay.addActionListener(this);
        btnPersonCust.addActionListener(this);
        btnCarCard.addActionListener(this);
        btnCarPay.addActionListener(this);
        btnCarCust.addActionListener(this);
        btnSelect.addActionListener(this);
        btnExport.addActionListener(this);
        btnImport.addActionListener(this);

        //设置年份
        cbYear.addItem("2018");
        cbYear.addItem("2019");
        cbYear.addItem("2020");
        cbYear.addItem("2021");
        cbYear.addItem("2022");
        //设置月份
        cbMonth.addItem("01");
        cbMonth.addItem("02");
        cbMonth.addItem("03");
        cbMonth.addItem("04");
        cbMonth.addItem("05");
        cbMonth.addItem("06");
        cbMonth.addItem("07");
        cbMonth.addItem("08");
        cbMonth.addItem("09");
        cbMonth.addItem("10");
        cbMonth.addItem("11");
        cbMonth.addItem("12");

        //默认禁用全部按钮
        hideButton(0);
        //年月选择，只要有一项选择，重置文件路径，防止导入导出月份不一致
        cbYear.addItemListener(new ItemListener() {
            @Override
            public void itemStateChanged(ItemEvent e) {
                hideButton(0);
                filePath = "";
                txtFileName.setText("");
            }
        });
        cbMonth.addItemListener(new ItemListener() {
            @Override
            public void itemStateChanged(ItemEvent e) {
                hideButton(0);
                filePath = "";
                txtFileName.setText("");
            }
        });
    }

    //按钮点击事件
    public void actionPerformed(ActionEvent e) {
        //年 2018
        String fieldYear = cbYear.getSelectedItem().toString();
        //月 08
        String fieldMonth = cbMonth.getSelectedItem().toString();
        //表字段 1808
        String tableName = fieldYear.substring(2, fieldYear.length()) + fieldMonth;
        //个人卡片
        if (e.getActionCommand().equals("btnPersonCard")) {
            if (filePath.equals("")) {
                JOptionPane.showMessageDialog(panel1, "请先选择要导入的文件");
                return;
            }
            loadSwingWorker = new LoadSwingWorker(filePath, 1, tableName, this);
            loadSwingWorker.execute();
        }
        //个人充值
        if (e.getActionCommand().equals("btnPersonPay")) {
            if (filePath.equals("")) {
                JOptionPane.showMessageDialog(panel1, "请先选择要导入的文件");
                return;
            }
            loadSwingWorker = new LoadSwingWorker(filePath, 2, tableName, this);
            loadSwingWorker.execute();
        }
        //个人消费
        if (e.getActionCommand().equals("btnPersonCust")) {
            if (filePath.equals("")) {
                JOptionPane.showMessageDialog(panel1, "请先选择要导入的文件");
                return;
            }
            loadSwingWorker = new LoadSwingWorker(filePath, 3, tableName, this);
            loadSwingWorker.execute();
        }
        //车队卡片
        if (e.getActionCommand().equals("btnCarCard")) {
            if (filePath.equals("")) {
                JOptionPane.showMessageDialog(panel1, "请先选择要导入的文件");
                return;
            }
            loadSwingWorker = new LoadSwingWorker(filePath, 4, tableName, this);
            loadSwingWorker.execute();
        }
        //车队充值
        if (e.getActionCommand().equals("btnCarPay")) {
            if (filePath.equals("")) {
                JOptionPane.showMessageDialog(panel1, "请先选择要导入的文件");
                return;
            }
            loadSwingWorker = new LoadSwingWorker(filePath, 5, tableName, this);
            loadSwingWorker.execute();
        }
        //车队消费
        if (e.getActionCommand().equals("btnCarCust")) {
            if (filePath.equals("")) {
                JOptionPane.showMessageDialog(panel1, "请先选择要导入的文件");
                return;
            }
            loadSwingWorker = new LoadSwingWorker(filePath, 6, tableName, this);
            loadSwingWorker.execute();
        }
        //文件选择
        if (e.getActionCommand().equals("btnSelect")) {
            //文件路径为空，重新选择，不为空，打开上次选择的文件夹
            if ("".equals(filePath)) {
                jf = new JFileChooser();
            } else {
                jf = new JFileChooser(filePath);
                //jf.changeToParentDirectory();
            }
            //只选择文件
            jf.setFileSelectionMode(JFileChooser.FILES_ONLY);
            jf.setAcceptAllFileFilterUsed(false);
            //添加文件过滤器
            if (csvRadioButton.isSelected()) {
                jf.addChoosableFileFilter(new CSVFileFilter());
            } else {
                jf.addChoosableFileFilter(new XLSXFileFilter());
            }
            jf.showOpenDialog(panel1);
            File f = jf.getSelectedFile();//使用文件类获取选择器选择的文件
            if (f != null && f.exists()) {
                filePath = f.getAbsolutePath();//返回路径名
                //JOptionPane弹出对话框类，显示绝对路径名
                txtFileName.setText(filePath);
                if (csvRadioButton.isSelected()) {
                    //如果是基础数据导入，判断文件名是否包含对应年份和月份，例如201808
                    if (f.getName().contains(fieldYear + fieldMonth)) {
                        switchBtn(f.getName(), fieldYear, fieldMonth);
                    } else {
                        //不包含则提示，规范数据，防止误操作
                        JOptionPane.showMessageDialog(panel1, "文件名日期与导入日期(" + fieldYear + fieldMonth + ")不一致，请核对导入文件");
                    }
                } else {
                    if (!f.getName().contains(fieldYear + "年" + fieldMonth + "月")) {
                        //本地导出的报表以年份月份开头，例如2018年08月，做数据库导入必须符合文件名限制
                        JOptionPane.showMessageDialog(panel1, "文件名日期与导入日期(" + fieldYear + fieldMonth + ")不一致，请核对导入文件");
                        //置空文件名
                        filePath = "";
                        txtFileName.setText("");
                    }
                }
            }
        }
        //数据导出到本地
        if (e.getActionCommand().equals("btnExport")) {
            int res = JOptionPane.showConfirmDialog(panel1, "即将导出 " + fieldYear + " 年 " + fieldMonth + " 月 月度数据报表,是否继续？", "请认真核对", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE);
            if (res == 0) {
                exportSwingWorker = new ExportSwingWorker(fieldYear, fieldMonth, tableName, this);
                exportSwingWorker.execute();
            }
        }

        //数据导入到数据库
        if (e.getActionCommand().equals("btnImport")) {
            if (filePath.equals("")) {
                JOptionPane.showMessageDialog(panel1, "请先选择要导入的文件");
                return;
            }
            int res = JOptionPane.showConfirmDialog(panel1, "准备将 " + fieldYear + " 年 " + fieldMonth + " 月 月度数据报表导入到正式数据库,是否继续？", "请认真核对无误后继续", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE);
            if (res == 0) {
                importSwingWorker = new ImportSwingWorker(filePath, fieldYear, fieldMonth, tableName, this);
                importSwingWorker.execute();
            }
        }
    }

    /*********************************** 数据导入开始**********************************/
    @Override
    public void onStart() {
        if (glasspane != null) {
            glasspane.start();//开始动画加载效果
        }
    }

    @Override
    public void onProcess(int i) {
        glasspane.setText("正在导入数据, 请稍后 ..." + i);
    }

    @Override
    public void onSuccess() {
        JOptionPane.showMessageDialog(panel1, "数据导入成功");
        hideButton(0);
        filePath = "";
        txtFileName.setText("");
    }

    @Override
    public void onFailed(String msg) {
        JOptionPane.showMessageDialog(panel1, msg);
    }

    @Override
    public void onFileNotFound() {

    }

    @Override
    public void onComplete() {
        glasspane.setText("正在导入数据, 请稍后 ...");
        glasspane.stop();//停止动画加载效果
    }

    /*********************************数据导入结束*******************************/


    /*********************************数据导出维护开始*******************************/
    @Override
    public void onExportStart() {
        glasspane.setText("正在导出数据, 请稍后 ...");
        if (glasspane != null) {
            glasspane.start();//开始动画加载效果
        }
    }

    @Override
    public void onExportProcess(String msg) {
        glasspane.setText(msg);
    }

    @Override
    public void onExportSuccess(String msg) {
        JOptionPane.showMessageDialog(panel1, msg);
    }

    @Override
    public void onExportError(String msg) {
        JOptionPane.showMessageDialog(panel1, msg);
    }

    @Override
    public void onExportComplete() {
        glasspane.stop();//停止动画加载效果
    }

    /*********************************数据导出维护结束*******************************/
    /**
     * 判断导入文件（必须符合文件名命名规范，否则不予导入）
     *
     * @param fileName 文件名
     * @param y        年
     * @param m        月
     */
    private void switchBtn(String fileName, String y, String m) {
        String content = "";
        int type = 0;
        if (fileName.contains("个人") && fileName.contains("充值")) {
            content = "个人充值数据";
            type = 2;
        } else if (fileName.contains("个人") && fileName.contains("消费")) {
            content = "个人消费数据";
            type = 3;
        } else if (fileName.contains("车队") && fileName.contains("充值")) {
            content = "车队充值数据";
            type = 5;
        } else if (fileName.contains("车队") && fileName.contains("消费")) {
            content = "车队消费数据";
            type = 6;
        } else if (fileName.contains("个人") && fileName.contains("信息")) {
            content = "个人办卡信息";
            type = 1;
        } else if (fileName.contains("车队") && fileName.contains("信息")) {
            content = "车队办卡信息";
            type = 4;
        } else {
            hideButton(0);
            JOptionPane.showMessageDialog(panel1, "无法识别的文件名，请规范导入文件");
        }
        if (type != 0) {
            int res = JOptionPane.showConfirmDialog(panel1, "将导入 " + y + " 年 " + m + " 月 " + content + ",是否继续？", "数据核对提示", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE);
            if (res == 0) {
                hideButton(type);
            } else {
                //如果取消，则置空文件
                hideButton(0);
                filePath = "";
                txtFileName.setText("");
            }
        }
    }

    //禁用按钮，防止点错
    private void hideButton(int i) {
        btnPersonCard.setEnabled(false);
        btnPersonPay.setEnabled(false);
        btnPersonCust.setEnabled(false);
        btnCarCard.setEnabled(false);
        btnCarPay.setEnabled(false);
        btnCarCust.setEnabled(false);
        switch (i) {
            case 1:
                btnPersonCard.setEnabled(true);
                break;
            case 2:
                btnPersonPay.setEnabled(true);
                break;
            case 3:
                btnPersonCust.setEnabled(true);
                break;
            case 4:
                btnCarCard.setEnabled(true);
                break;
            case 5:
                btnCarPay.setEnabled(true);
                break;
            case 6:
                btnCarCust.setEnabled(true);
                break;
        }
    }

/*    public static void main(String[] args) {
        JFrame frame = new JFrame("DataFormMain");
        frame.setResizable(false);
        JPanel rootPane = new DataFormMain().panel1;
        frame.setContentPane(rootPane);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setSize(600, 400);
        frame.setLocationRelativeTo(rootPane);//居中
        frame.setVisible(true);
    }*/
}
