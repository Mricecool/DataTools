package com.datatools;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class DataFormLogin implements ActionListener {
    private JTextField txtLogin;
    private JTextField txtPwd;
    private JButton btnLogin;
    private JButton btnClose;
    private JPanel panelMain;
    private JPanel panelFoot;
    private JLabel lableLogin;
    private JLabel lablePwd;

    public DataFormLogin() {
        JFrame frame = new JFrame("数据导入导出工具-登录");
        frame.setResizable(false);
        frame.setContentPane(panelMain);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setSize(300, 200);
        frame.setIconImage(Toolkit.getDefaultToolkit().getImage(DataFormLogin.class.getResource("/image/logo.png")));
        frame.setLocationRelativeTo(panelMain);//居中
        frame.setVisible(true);

        btnLogin.setActionCommand("btnLogin");
        btnClose.setActionCommand("btnClose");
        btnClose.addActionListener(this);
        btnLogin.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String name = txtLogin.getText().toString().trim();
                if (name == null || name.length() == 0) {
                    JOptionPane.showMessageDialog(panelMain, "请输入登录名");
                    return;
                }
                String pwd = txtPwd.getText().toString().trim();
                if (pwd == null || pwd.length() == 0) {
                    JOptionPane.showMessageDialog(panelMain, "请输入登录密码");
                    return;
                }
                //默认管理员密码
                if("admin".equals(name)&&"zsy!@#123".equals(pwd)){
                    new DataFormMain();
                    frame.dispose();
                }else{
                    JOptionPane.showMessageDialog(panelMain, "用户名或密码错误！");
                }
            }
        });
    }

    @Override
    public void actionPerformed(ActionEvent e) {
        if (e.getActionCommand().equals("btnClose")) {
            System.exit(0);//终止当前程序
        }
    }

    public static void main(String[] args) {
        try {
            //采用当前系统样式
            String lookAndFeel =UIManager.getSystemLookAndFeelClassName();
            UIManager.setLookAndFeel(lookAndFeel);
        }catch (Exception ex){

        }
        new DataFormLogin();
    }
}
