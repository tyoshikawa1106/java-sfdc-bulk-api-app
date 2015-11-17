package com.example;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import java.util.ResourceBundle;

import java.io.*;
import java.util.*;
import com.sforce.async.*;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import org.codehaus.jackson.ObjectCodec;

@SuppressWarnings("unused")
@EnableAutoConfiguration
@ComponentScan
public class App implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }

    @Override
    public void run(String... strings) throws Exception {
        // resources→conf→userInfo.propertiesから設定情報取得
        ResourceBundle resouce = ResourceBundle.getBundle("conf.userInfo");
        UserInfo userInfo = new UserInfo();
        // バッチ実行
        this.runDataImport("Account", userInfo);
    }

    public void runDataImport(String sobjectType, UserInfo userInfo) throws AsyncApiException, ConnectionException, IOException {
        System.out.println("-- runDataImport --");
        // ConnectorConfig情報を作成
        ConnectorConfig partnerConfig = this.getConnectorConfig(userInfo);
        
        // BulkAPIを実行するための接続情報を作成
        BulkConnection connection = this.getBulkConnection(userInfo, partnerConfig);
        // ジョブを作成
        JobInfo job = this.createUpsertJob(sobjectType, connection, "Id");
        
        // CSVファイルから登録データ情報を取得してジョブバッチを作成
        List<BatchInfo> batchInfoList = this.createBatchesFromCSVFile(connection, job, userInfo);
        // ジョブのステータスをクローズにする
        this.closeJob(connection, job.getId());
        // ジョブが完了するまで待機
        this.awaitCompletion(connection, job, batchInfoList);
        // エラーの操作の結果をチェック
        this.checkResults(connection, job, batchInfoList);
    }
    
    private ConnectorConfig getConnectorConfig(UserInfo userInfo) throws ConnectionException, AsyncApiException {
        System.out.println("-- getConnectorConfig --");
        // ConnectorConfigでセッション情報を保存
        ConnectorConfig partnerConfig = new ConnectorConfig();
        partnerConfig.setUsername(userInfo.userId);
        partnerConfig.setPassword(userInfo.password);
        partnerConfig.setAuthEndpoint(userInfo.authEndpoint);
        new PartnerConnection(partnerConfig);
        
        return partnerConfig;
    }
    
    private BulkConnection getBulkConnection(UserInfo userInfo, ConnectorConfig partnerConfig) throws ConnectionException, AsyncApiException {
        System.out.println("-- getBulkConnection --");
        // ConnectorConfigにセッションIDをセット。BulkConnectionで使用する。
        ConnectorConfig config = new ConnectorConfig();
        config.setSessionId(partnerConfig.getSessionId());
        
        // Bulk APIの接続情報を作成
        String soapEndpoint = partnerConfig.getServiceEndpoint();
        String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/")) + "async/" + userInfo.apiVersion;
        config.setRestEndpoint(restEndpoint);
        config.setCompression(true);   // Debugしたいときは「false」
        config.setTraceMessage(false); // トレースメッセージを確認したいときは「true」
        
        // Bulk API接続
        BulkConnection connection = new BulkConnection(config);
        return connection;
    }
    
    private JobInfo createUpsertJob(String sobjectType, BulkConnection connection, String externalIdFieldName) throws AsyncApiException {
        System.out.println("-- createUpsertJob --");
        JobInfo job = new JobInfo();
        job.setObject(sobjectType);
        job.setOperation(OperationEnum.upsert);
        job.setExternalIdFieldName(externalIdFieldName);
        job.setContentType(ContentType.CSV);
        job = connection.createJob(job);
        return job;
    }
    
    private List<BatchInfo> createBatchesFromCSVFile(BulkConnection connection, JobInfo jobInfo, UserInfo userInfo) throws IOException, AsyncApiException {
        System.out.println("-- createBatchesFromCSVFile --");
        
        List<BatchInfo> batchInfos = new ArrayList<BatchInfo>();
        
        // ファイル読み込み情報作成
        BufferedReader rdr = new BufferedReader(
            new InputStreamReader(new FileInputStream(userInfo.filePath))
        );
        
        // CSVのヘッダー行を読み込み
        byte[] headerBytes = (rdr.readLine() + "\n").getBytes("UTF-8");
        int headerBytesLength = headerBytes.length;
        
        // ファイル情報作成
        File tmpFile = File.createTempFile("bulkAPIInsert", ".csv");
        
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
    
    private void closeJob(BulkConnection connection, String jobId) throws AsyncApiException {
        System.out.println("-- closeJob --");
        JobInfo job = new JobInfo();
        job.setId(jobId);
        job.setState(JobStateEnum.Closed);
        connection.updateJob(job);
    }
    
    private void awaitCompletion(BulkConnection connection, JobInfo job, List<BatchInfo> batchInfoList) throws AsyncApiException {
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

    private void checkResults(BulkConnection connection, JobInfo job, List<BatchInfo> batchInfoList) throws AsyncApiException, IOException {
        System.out.println("-- checkResults --");
        
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
                boolean created = Boolean.valueOf(resultInfo.get("Created"));

                String id = resultInfo.get("Id");
                String error = resultInfo.get("Error");
                if (success && created) {
                    System.out.println("Created row with id " + id);
                } else if (!success) {
                    System.out.println("Failed with error: " + error);
                }
            }
        }
    }

    private void createBatch(FileOutputStream tmpOut, File tmpFile, List<BatchInfo> batchInfos, BulkConnection connection, JobInfo jobInfo) throws IOException, AsyncApiException {
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

    public class UserInfo {
        String userId;
        String password;
        String apiVersion;
        String authEndpoint;
        String filePath;

        public UserInfo() {
            ResourceBundle resouce = ResourceBundle.getBundle("conf.userInfo");
            this.userId = resouce.getString("userId");
            this.password = resouce.getString("password");
            this.apiVersion = resouce.getString("apiVersion");
            this.authEndpoint = resouce.getString("authEndpoint") + apiVersion;
            this.filePath = resouce.getString("filePath");
        }
    }
}
