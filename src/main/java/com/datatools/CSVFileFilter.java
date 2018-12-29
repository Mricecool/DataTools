package com.datatools;

import javax.swing.filechooser.FileFilter;
import java.io.File;

/**
 * CSV文件过滤
 */
public class CSVFileFilter extends FileFilter {

    public boolean accept(File f) {
        if (f.isDirectory()) {
            return true;
        }

        String fileName = f.getName();
        int index = fileName.lastIndexOf('.');
        if (index > 0 && index < fileName.length() - 1) {
            String extension = fileName.substring(index + 1).toLowerCase();
            if (extension != null) {
                if (extension.equals("csv")) {
                    return true;
                } else {
                    return false;
                }
            }
        }
        return false;
    }
        public String getDescription () {
            return "CSV文件(*.csv)";
        }
    }
