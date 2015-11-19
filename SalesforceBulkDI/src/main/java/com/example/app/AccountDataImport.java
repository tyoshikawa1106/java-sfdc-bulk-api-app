package com.example.app;

import com.example.app.SalesforceApiUtil;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.sforce.async.AsyncApiException;
import com.sforce.async.BatchInfo;
import com.sforce.async.BulkConnection;
import com.sforce.async.CSVReader;
import com.sforce.async.JobInfo;
import com.sforce.soap.partner.Error;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

public class AccountDataImport {
	
    private SalesforceApiUtil sfdcApiUtil = new SalesforceApiUtil();
    private int skipErrorCount = 0;
	
    /**
     * データインポート処理
     * @param sobjectType オブジェクトAPI名
     * @param userInfo ユーザ情報
     * @throws AsyncApiException
     * @throws ConnectionException
     * @throws IOException
     */
    public void runDataImport(String sobjectType, UserInfo userInfo) throws AsyncApiException, ConnectionException, IOException {
        System.out.println("-- runDataImport --");
        // ConnectorConfig情報を作成
        ConnectorConfig partnerConfig = this.sfdcApiUtil.getConnectorConfig(userInfo);
        
        // BulkAPIを実行するための接続情報を作成
        BulkConnection connection = this.sfdcApiUtil.getBulkConnection(userInfo, partnerConfig);
        // ジョブを作成
        JobInfo job = this.sfdcApiUtil.createUpsertJob(sobjectType, connection, "Id");
        
        // ファイル読み込み情報作成
        BufferedReader rdr = this.getBufferedReader(userInfo.filePath);
        // CSVのヘッダー行を読み込み
        byte[] headerBytes = (rdr.readLine() + "\n").getBytes("UTF-8");
        // ヘッダー行を置換
        headerBytes = this.doCsvHeaderReplace(headerBytes);
        
        // CSVファイルから登録データ情報を取得してジョブバッチを作成
        List<BatchInfo> batchInfoList = this.sfdcApiUtil.createBatchesFromCSVFile(connection, job, rdr, headerBytes);
        // ジョブのステータスをクローズにする
        this.sfdcApiUtil.closeJob(connection, job.getId());
        // ジョブが完了するまで待機
        this.sfdcApiUtil.awaitCompletion(connection, job, batchInfoList);

        // 最新のジョブ情報を取得
        JobInfo resultJob = connection.getJobStatus(job.getId());
        // エラーの操作の結果をチェック
        this.checkResults(connection, resultJob, batchInfoList);
        // エラー件数のチェック
        Boolean isError = this.isErrorRecords(resultJob);
        // 異常なエラーが発生している場合、エラーオブジェクトに登録
        if (isError) {
            System.out.println("異常なエラーです : 【" + resultJob.getId() + "】");
            System.exit(1);
        }
        
        // ジョブ実行通知レコードを作成
        String taskId = this.createTask(partnerConfig, resultJob);
        System.out.println("Create Task = " + taskId);
    }
    
    /**
     * 指定したファイルを条件にBufferedReader変数を取得
     * @param filePath ファイルパス
     * @return BufferedReader変数
     * @throws FileNotFoundException
     */
    private BufferedReader getBufferedReader(String filePath) throws FileNotFoundException {
    	BufferedReader rdr = new BufferedReader(
            new InputStreamReader(new FileInputStream(filePath))
        );
    	return rdr;
    }

    /**
     * ヘッダー行の置換処理
     * @param headerBytes ヘッダー行
     * @return 置換結果
     * @throws IOException
     */
    private byte[] doCsvHeaderReplace(byte[] headerBytes) throws IOException {
        String headerStr = new String(headerBytes,"UTF-8");
        headerStr = headerStr.replace("ACCOUNT_NO", "ACCOUNTNUMBER");
        return headerStr.getBytes();
    }

    /**
     * 処理結果チェック
     * @param connection BulkAPIの接続情報
     * @param job ジョブ情報
     * @param batchInfoList バッチ情報
     * @throws AsyncApiException
     * @throws IOException
     */
    private void checkResults(BulkConnection connection, JobInfo job, List<BatchInfo> batchInfoList) throws AsyncApiException, IOException {
        System.out.println("-- checkResults --");

        // 対象外エラー情報取得
        ArrayList<String> skipErrorList = this.getSkipErrorList();
        
        // バッチ情報をループしてエラーチェック
        for (BatchInfo b : batchInfoList) {
            CSVReader rdr = new CSVReader(connection.getBatchResultStream(job.getId(), b.getId()));
            List<String> resultHeader = rdr.nextRecord();
            int resultCols = resultHeader.size();
            List<String> row;
            while ((row = rdr.nextRecord()) != null) {
                Map<String, String> resultInfo = new HashMap<String, String>();
                for (int i = 0; i < resultCols; i++) {
                    resultInfo.put(resultHeader.get(i), row.get(i));
                }
                boolean success = Boolean.valueOf(resultInfo.get("Success"));
                String error = resultInfo.get("Error");

                if (!success) {
                    // 発生しても問題ないエラーの件数をカウント
                    Boolean isSkip = this.isSkipError(error, skipErrorList);
                    if (isSkip) {
                        this.skipErrorCount++;
                    }
                }
            }
        }
    }

    /**
     * 発生したエラーが検知不要のエラー情報と一致するか判定
     * @param error エラーメッセージ
     * @param skipErrorList 検知不要エラー情報
     * @return 判定結果
     */
    private Boolean isSkipError(String error, List<String> skipErrorList) {
        for (String key : skipErrorList) {
            if (error.indexOf(key) != -1) {
                return true;
            }
        }
        return false;
    }

    /**
     * 検知エラー情報の取得
     * @return 検知エラー情報 
     */
    private ArrayList<String> getSkipErrorList() {
        // 検知不要のエラー情報(エラーメッセージ)を取得
        ArrayList<String> skipErrorList = new ArrayList<String>();
        skipErrorList.add("[Allowable Error]");
        return skipErrorList;
    }
    
    /**
     * ジョブのエラー件数と検知不要エラー件数の比較
     * @param resultJob ジョブ情報
     * @return 比較結果
     */
    private Boolean isErrorRecords(JobInfo resultJob) {
        // 異常なエラーが発生しているか確認
        if (resultJob.getNumberRecordsFailed() != this.skipErrorCount) {
            return true;
        } else {
            return false;
        }
    }
    
    /**
     * ジョブ通知用のタスクレコードを作成
     * @param partnerConfig セッション情報
     * @param job ジョブ情報
     * @return レコードID
     * @throws ConnectionException
     */
    private String createTask(ConnectorConfig partnerConfig, JobInfo job) throws ConnectionException {
        PartnerConnection partnerConnection = null;
        String result = null;
        try {
            // PartnerConnectionを作成
            partnerConnection = com.sforce.soap.partner.Connector.newConnection(partnerConfig);
            
            // 現在の日時を取得
            Date date = new Date();
            
            // Task Objectを作成
            SObject task = new SObject();
            task.setType("Task");
            task.setField("Subject", "取引先インポートバッチ実行通知");
            task.setField("ActivityDate", date);
            task.setField("Priority", "High");
            task.setField("Description", "取引先インポートバッチが実行されました。\n取り込みジョブを確認してください。\n【ジョブID = " + job.getId() + "】");
            // sObjectのリストに追加
            SObject[] tasks = new SObject[1];
            tasks[0] = task;
            // INSERTを実行
            SaveResult[] results = partnerConnection.create(tasks);

            // 処理結果を判定
            for (int j = 0; j < results.length; j++) {
                if (results[j].isSuccess()) {
                    result = results[j].getId();
                } else {
                    for (int i = 0; i < results[j].getErrors().length; i++) {
                        Error err = results[j].getErrors()[i];
                        System.out.println("Errors were found on item " + j);
                        System.out.println("Error code: " + err.getStatusCode().toString());
                        System.out.println("Error message: " + err.getMessage());
                    }
                }
            }
        } catch (ConnectionException e) {
            System.out.println("<< ConnectionException >> " + e.getMessage());
        } finally {
            partnerConnection.logout();
        }
        return result;
    }
}
