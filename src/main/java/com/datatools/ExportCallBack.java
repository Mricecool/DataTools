package com.datatools;

/**
 * 数据导入导出回调接口
 */
public interface ExportCallBack {

    void onExportStart();

    void onExportProcess(String msg);

    void onExportSuccess(String msg);

    void onExportError(String msg);

    void onExportComplete();

}
