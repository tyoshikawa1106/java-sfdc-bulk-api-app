package com.example.app;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

public class UserInfo {
    public String userId;
    public String password;
    public String apiVersion;
    public String authEndpoint;
    public String filePath;

    /**
     * コンストラクタ
     */
    public UserInfo() {
        try {
            InputStream in = new BufferedInputStream(new FileInputStream("./conf/userInfo.properties"));
            ResourceBundle resouce = new PropertyResourceBundle(in);
            this.userId = resouce.getString("userId");
            this.password = resouce.getString("password");
            this.apiVersion = resouce.getString("apiVersion");
            this.authEndpoint = resouce.getString("authEndpoint") + apiVersion;
            this.filePath = resouce.getString("filePath");
        } catch (FileNotFoundException e) {
            System.out.println("<< FileNotFoundException >> " + e.getMessage());
        } catch (IOException e) {
            System.out.println("<< IOException >> " + e.getMessage());
        }
    }
}