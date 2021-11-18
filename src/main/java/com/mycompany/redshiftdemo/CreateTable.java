/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.redshiftdemo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 *
 * @author Naveen
 */
public class CreateTable {
    static final String dbURL = "jdbc:redshift://myredshiftcluster.ctqtd45s8xvi.us-east-1.redshift.amazonaws.com:5439/mytestdb";
    static final String MasterUsername = "awsuser";
    static final String MasterUserPassword = "Awsredshiftpassword1";
    
    private static final String SQL_CREATE = 
        "create table employees(id integer, name varchar(25), age integer);";

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        
        try{
           //Dynamically load driver at runtime.
           //Redshift JDBC 4.1 driver: com.amazon.redshift.jdbc41.Driver
           //Redshift JDBC 4 driver: com.amazon.redshift.jdbc4.Driver
           Class.forName("com.amazon.redshift.jdbc.Driver");

           //Open a connection and define properties.
           System.out.println("Connecting to database...");
           Properties props = new Properties();

           //Uncomment the following line if using a keystore.
           //props.setProperty("ssl", "true");
           props.setProperty("user", MasterUsername);
           props.setProperty("password", MasterUserPassword);
           conn = DriverManager.getConnection(dbURL, props);
           System.out.println("Connecetd to database");
           
           System.out.println("Creating Table Employee..");
            try {
                stmt = conn.createStatement();
                stmt.executeUpdate(SQL_CREATE);
                System.out.println("Employee Table Created.");
            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            } finally {
                
            }
        
           }catch(Exception ex){
           //For convenience, handle all errors here.
           ex.printStackTrace();
        }finally{
           //Finally block to close resources.
           try{
              if(stmt!=null)
                 stmt.close();
           }catch(Exception ex){
           }// nothing we can do
           try{
              if(conn!=null)
                 conn.close();
           }catch(Exception ex){
              ex.printStackTrace();
           }
        }
        System.out.println("Finished connectivity test.");
     }
}
