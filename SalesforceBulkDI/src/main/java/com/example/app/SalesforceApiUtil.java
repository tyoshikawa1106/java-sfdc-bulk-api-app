package com.example.app;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import com.sforce.async.AsyncApiException;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.JobStateEnum;
import com.sforce.async.OperationEnum;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

public class SalesforceApiUtil {
	
	/**
     * ユーザ情報の値存在判定
     * @param userInfo ユーザ情報
     * @return 判定結果
     */
    public Boolean isEmptyUserInfo(UserInfo userInfo) {
        if (userInfo.userId.isEmpty()) {
            System.out.println("<< ERROR >> ユーザIDの取得に失敗しました。userInfo.propertiesが正しく設定されているか確認してください。");
            return true;
        } else if (userInfo.password.isEmpty()) {
            System.out.println("<< ERROR >> パスワードの取得に失敗しました。userInfo.propertiesが正しく設定されているか確認してください。");
            return true;
        } else if (userInfo.apiVersion.isEmpty()) {
            System.out.println("<< ERROR >> APIバージョンの取得に失敗しました。userInfo.propertiesが正しく設定されているか確認してください。");
            return true;
        } else if (userInfo.authEndpoint.isEmpty()) {
            System.out.println("<< ERROR >> 接続URLの取得に失敗しました。userInfo.propertiesが正しく設定されているか確認してください。");
            return true;
        } else if (userInfo.filePath.isEmpty()) {
            System.out.println("<< ERROR >> ファイルパスの取得に失敗しました。userInfo.propertiesが正しく設定されているか確認してください。");
            return true;
        }

        return false;
    }
	
	/**
     * ConnectorConfigにセッション情報を保存
     * @param userInfo ユーザ情報
     * @return ConnectorConfig
     * @throws ConnectionException
     * @throws AsyncApiException
     */
    public ConnectorConfig getConnectorConfig(UserInfo userInfo) throws ConnectionException, AsyncApiException {
        System.out.println("-- getConnectorConfig --");
        ConnectorConfig partnerConfig = new ConnectorConfig();
        partnerConfig.setUsername(userInfo.userId);
        partnerConfig.setPassword(userInfo.password);
        partnerConfig.setAuthEndpoint(userInfo.authEndpoint);
        new PartnerConnection(partnerConfig);
        return partnerConfig;
    }
    
    /**
     * BulkAPI接続情報取得
     * @param userInfo ユーザ情報
     * @param partnerConfig セッション情報
     * @return BulkConnection
     * @throws ConnectionException
     * @throws AsyncApiException
     */
    public BulkConnection getBulkConnection(UserInfo userInfo, ConnectorConfig partnerConfig) throws ConnectionException, AsyncApiException {
        System.out.println("-- getBulkConnection --");
        // ConnectorConfigにセッションIDをセット。BulkConnectionで使用する。
        ConnectorConfig config = new ConnectorConfig();
        config.setSessionId(partnerConfig.getSessionId());
        
        // BulkAPIの接続情報を作成
        String soapEndpoint = partnerConfig.getServiceEndpoint();
        String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/")) + "async/" + userInfo.apiVersion;
        config.setRestEndpoint(restEndpoint);
        config.setCompression(true);   // Debugしたいときは「false」
        config.setTraceMessage(false); // トレースメッセージを確認したいときは「true」
        
        // BulkAPI接続
        BulkConnection connection = new BulkConnection(config);
        return connection;
    }
    
    /**
     * ジョブの作成
     * @param sobjectType オブジェクトAPI名
     * @param connection Bulk API接続情報
     * @param externalIdFieldName 外部ID項目のAPI名
     * @return ジョブ情報
     * @throws AsyncApiException
     */
    public JobInfo createUpsertJob(String sobjectType, BulkConnection connection, String externalIdFieldName) throws AsyncApiException {
        System.out.println("-- createUpsertJob --");
        JobInfo job = new JobInfo();
        job.setObject(sobjectType);
        job.setOperation(OperationEnum.upsert);
        job.setExternalIdFieldName(externalIdFieldName);
        job.setContentType(ContentType.CSV);
        job = connection.createJob(job);
        return job;
    }
    
    /**
     * ジョブのクローズ
     * @param connection BulkAPIの接続情報
     * @param jobId ジョブID
     * @throws AsyncApiException
     */
    public void closeJob(BulkConnection connection, String jobId) throws AsyncApiException {
        System.out.println("-- closeJob --");
        JobInfo job = new JobInfo();
        job.setId(jobId);
        job.setState(JobStateEnum.Closed);
        connection.updateJob(job);
    }
    
    /**
     * ジョブのクローズ操作が反映されるまで待機
     * @param connection BulkAPIの接続情報
     * @param job ジョブ情報
     * @param batchInfoList バッチ情報
     * @throws AsyncApiException
     */
    public void awaitCompletion(BulkConnection connection, JobInfo job, List<BatchInfo> batchInfoList) throws AsyncApiException {
        System.out.println("-- awaitCompletion --");
        
        long sleepTime = 0L;
        Set<String> incomplete = new HashSet<String>();
        for (BatchInfo bi : batchInfoList) {
            incomplete.add(bi.getId());
        }
        while (!incomplete.isEmpty()) {
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {}
                System.out.println("Awaiting results..." + incomplete.size());
                sleepTime = 10000L;
                BatchInfo[] statusList = connection.getBatchInfoList(job.getId()).getBatchInfo();
                for (BatchInfo b : statusList) {
                if (b.getState() == BatchStateEnum.Completed || b.getState() == BatchStateEnum.Failed) {
                    if (incomplete.remove(b.getId())) {
                        System.out.println("BATCH STATUS:\n" + b);
                    }
                }
            }
        }
    }
    
    /**
     * CSVの読み込みとバッチの作成
     * @param connection
     * @param jobInfo
     * @param userInfo
     * @return バッチ情報
     * @throws IOException
     * @throws AsyncApiException
     */
    public List<BatchInfo> createBatchesFromCSVFile(BulkConnection connection, JobInfo jobInfo, BufferedReader rdr, byte[] headerBytes) throws IOException, AsyncApiException {
        System.out.println("-- createBatchesFromCSVFile --");
        
        List<BatchInfo> batchInfos = new ArrayList<BatchInfo>();
        int headerBytesLength = headerBytes.length;
        
        // ファイル情報作成
        File tmpFile = File.createTempFile("tmpCsvFile", ".csv");
        
        // CSVをバッチ実行用に分割
        try {
            FileOutputStream tmpOut = new FileOutputStream(tmpFile);
            int maxBytesPerBatch = 10000000; // バッチあたり10000000バイト
            int maxRowsPerBatch = 10000;     // バッチあたり1000行
            int currentBytes = 0;
            int currentLines = 0;
            
            String nextLine;
            while ((nextLine = rdr.readLine()) != null) {
                byte[] bytes = (nextLine + "\n").getBytes("UTF-8");
                // 指定したバッチサイズの上限に達した時に新しいバッチを作成
                if (currentBytes + bytes.length > maxBytesPerBatch || currentLines > maxRowsPerBatch) {
                    this.createBatch(tmpOut, tmpFile, batchInfos, connection, jobInfo);
                    currentBytes = 0;
                    currentLines = 0;
                }
                if (currentBytes == 0) {
                    tmpOut = new FileOutputStream(tmpFile);
                    // 置換したヘッダーをBytesに変換して処理を実行
                    tmpOut.write(headerBytes);
                    currentBytes = headerBytesLength;
                    currentLines = 1;
                }
                tmpOut.write(bytes);
                currentBytes += bytes.length;
                currentLines++;
            }
            
            // 残りの行をバッチ実行して処理終了
            if (currentLines > 1) {
                this.createBatch(tmpOut, tmpFile, batchInfos, connection, jobInfo);
            }
        } finally {
            tmpFile.delete();
            rdr.close();
        }
        return batchInfos;
    }
    
    /**
     * バッチの作成
     * @param tmpOut ファイル出力情報
     * @param tmpFile ファイル情報
     * @param batchInfos バッチ情報
     * @param connection BulkAPIの接続情報
     * @param jobInfo ジョブ情報
     * @throws IOException
     * @throws AsyncApiException
     */
    public void createBatch(FileOutputStream tmpOut, File tmpFile, List<BatchInfo> batchInfos, BulkConnection connection, JobInfo jobInfo) throws IOException, AsyncApiException {
        System.out.println("-- createBatch --");
        tmpOut.flush();
        tmpOut.close();
        FileInputStream tmpInputStream = new FileInputStream(tmpFile);
        try {
            BatchInfo batchInfo = connection.createBatchFromStream(jobInfo, tmpInputStream);
            batchInfos.add(batchInfo);
        } finally {
            tmpInputStream.close();
        }
    }
}
