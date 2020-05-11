import java.sql.*;
import java.util.ArrayList;

//java SalaryStdDev sample employee wta 120183473
public class SalaryStdDev 
{
  public static void main(String[] args) 
  {
    String urlPrefix = "jdbc:db2://localhost:50000/";
    String url;
    String user;
    String password;
    String salary;
    String databaseName;
    Connection con;
    Statement stmt;
    ResultSet rs;
    ArrayList<Double> salaries = new ArrayList<Double>();
    
    if (args.length!=4)
    {
      System.exit(1);
    }
    url = urlPrefix + args[0];
    databaseName = args[1];
    user = args[2];
    password = args[3];
    try 
    {                                                                        
      // Load the driver
      Class.forName("com.ibm.db2.jcc.DB2Driver");

      // Create the connection using the IBM Data Server Driver for JDBC and SQLJ
      con = DriverManager.getConnection (url, user, password);
      // Commit changes manually
      con.setAutoCommit(false);
      // Create the Statement
      stmt = con.createStatement();
      // Execute a query and generate a ResultSet instance
      rs = stmt.executeQuery("SELECT salary FROM "+databaseName);
      // Print all of the employee numbers to standard output device
      while (rs.next()) {
    	  salary = rs.getString(1);
    	  salaries.add(Double.valueOf(salary));
      }
      // Close the ResultSet
      rs.close();   
      // Close the Statement
      stmt.close();
      // Connection must be on a unit-of-work boundary to allow close
      con.commit();
      // Close the connection
      con.close();
      
      
      Double sum = 0.0;
      for(Double element:salaries){
      	sum+=element;
      }
      double avg = sum/salaries.size();
      
      double total=0;
	  for(int i=0;i<salaries.size();i++){
		total += (salaries.get(i)-avg)*(salaries.get(i)-avg);
	  }
	  double standardDeviation = Math.sqrt(total/salaries.size()); 
	  System.out.println(standardDeviation);
      
    }
    catch (ClassNotFoundException e)
    {
      System.err.println("Could not load JDBC driver");
      System.out.println("Exception: " + e);
      e.printStackTrace();
    }

    catch(SQLException ex)
    {
      System.err.println("SQLException information");
      while(ex!=null) {
        System.err.println ("Error msg: " + ex.getMessage());
        System.err.println ("SQLSTATE: " + ex.getSQLState());
        System.err.println ("Error code: " + ex.getErrorCode());
        ex.printStackTrace();
        ex = ex.getNextException(); // For drivers that support chained exceptions
      }
    }
  }  // End main
}    // End EzJava