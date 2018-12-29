package com.datatools;

/**
 * 基本数据导入回调接口
 */
public interface SwingWorkerCallBack {

    void onStart();

    void onProcess(int i);

    void onSuccess();

    void onComplete();

    void onFailed(String msg);

    void onFileNotFound();

}
