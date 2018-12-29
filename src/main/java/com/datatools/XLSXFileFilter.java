package com.datatools;

import javax.swing.filechooser.FileFilter;
import java.io.File;

public class XLSXFileFilter extends FileFilter {

    public boolean accept(File f) {
        if (f.isDirectory()) {
            return true;
        }

        String fileName = f.getName();
        int index = fileName.lastIndexOf('.');
        if (index > 0 && index < fileName.length() - 1) {
            String extension = fileName.substring(index + 1).toLowerCase();
            if (extension != null) {
                if (extension.equals("xlsx")) {
                    return true;
                } else {
                    return false;
                }
            }
        }
        return false;
    }

    public String getDescription() {
        return "XLSX文件(*.xlsx)";
    }
}

