package excelautomation;

import com.weborders.utilities.Driver;
import org.testng.annotations.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JDBBCCONECTION {
    String oracledbUrl="jdbc:oracle:thin:@ ec2-3-91-252-169.compute-1.amazonaws.com:1521:xe";
    String oracleDbUserName="Hr";
    String oracledbPassword="hr";

    @Test
    public void oracleJDBC() throws SQLException {

        Connection connection= DriverManager.getConnection(oracledbUrl,oracleDbUserName,oracledbPassword);
     //   Statement statement=connection.createStatement();
        Statement statement=connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
        ResultSet resultSet=statement.executeQuery("select * from countries");

//       while(resultSet.next()){
//           System.out.println(resultSet.getObject(1)+ " -" + resultSet.getString("country_name") + "-" + resultSet.getString("region_id"));
//       }


        //find out how many records in the resultset
        resultSet.last();
        int rowsCount=resultSet.getRow();
        System.out.println("number of rows:" + rowsCount);

        resultSet.first();
        while(resultSet.next()){
         System.out.println(resultSet.getObject(1)+ " -" + resultSet.getString("country_name") + "-" + resultSet.getString("region_id"));
      }

        resultSet.close();
         statement.close();
         connection.close();

    }


@Test
public void jdbcMetadata() throws SQLException {

    Connection connection= DriverManager.getConnection(oracledbUrl,oracleDbUserName,oracledbPassword);
    //   Statement statement=connection.createStatement();
    Statement statement=connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);

    String sql="select  employee_id ,last_name ,job_id ,salary from employees";;
            //"select * from employees";


    ResultSet resultSet=statement.executeQuery(sql);

    //Database metadata
    DatabaseMetaData dbMetadata=connection.getMetaData();
    System.out.println("User :" + dbMetadata.getUserName());
    System.out.println("Database type :" + dbMetadata.getDatabaseProductName());

    //resultSet metadata

    ResultSetMetaData rsMedata=resultSet.getMetaData();
    System.out.println("Column count : " + rsMedata.getColumnCount());
    System.out.println(rsMedata.getColumnName(1));

    //print all column names using a loop
    for(int i=1;i<rsMedata.getColumnCount();i++){
        System.out.println( i + " -->" +rsMedata.getColumnName(i));
    }


      //Throw resultSet into a list of maps
    //create a list of map

    List<Map<String,Object>>list=new ArrayList<>();
    ResultSetMetaData rsMdata=resultSet.getMetaData();

    int colCount=rsMdata.getColumnCount();
    while(resultSet.next()){
        Map<String,Object>rowMap=new HashMap<>();
        for(int col=1;col<=colCount;col++){
            rowMap.put(rsMdata.getColumnName(col),resultSet.getObject(col));
        }
        list.add(rowMap);
    }

//print all employee ids from a list of map
    for(Map<String,Object>emp:list){
        System.out.println(emp.get("EMPLOYEE_ID")+"-->"+ emp.get("LAST_NAME"));

    }


    resultSet.close();
    statement.close();
    connection.close();

}





}
