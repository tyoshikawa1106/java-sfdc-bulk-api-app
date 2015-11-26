package com.example;

import com.example.app.UserInfo;
import com.example.app.SalesforceApiUtil;
import com.example.app.AccountDataImport;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import java.io.*;
import java.util.*;
import com.sforce.async.*;
import com.sforce.soap.partner.*;
import com.sforce.soap.partner.sobject.*;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.Error;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import org.codehaus.jackson.ObjectCodec;

@SuppressWarnings("unused")
@EnableAutoConfiguration
@ComponentScan
public class App implements CommandLineRunner {
    
    private SalesforceApiUtil sfdcApiUtil = new SalesforceApiUtil();
    private AccountDataImport accountDataImport = new AccountDataImport();

    /**
     * main
     */
    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }
    
    /**
     * run
     * @throws Exception
     */
    @Override
    public void run(String... strings) throws Exception {
        try {
            // ユーザ情報取得
            UserInfo userInfo = new UserInfo();
            // 値存在判定
            Boolean isError = this.sfdcApiUtil.isEmptyUserInfo(userInfo);
            // エラー発生時に処理終了
            if (isError) {
                System.exit(1);
            }
            // ファイル読み込み情報作成
            BufferedReader rdr = this.accountDataImport.getBufferedReader(userInfo.filePath);
            // 取引先インポートバッチ実行
            this.accountDataImport.runDataImport("Account", userInfo, rdr);
        } catch(Exception e) {
            System.out.println("<< Exception >> " + e);
            System.exit(1);
        }

        System.exit(0);
    }
}
