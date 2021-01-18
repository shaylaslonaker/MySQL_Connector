using MySql.Data.MySqlClient;
using System;
using System.Configuration;
using System.Data;
using System.IO;
using System.Collections.Generic;
using System.Dynamic;

namespace QuickFish.DataAccess
{
    public class BaseData
    {
        public string Connection()
        {
            string strReturn = "";
            strReturn = ConfigurationManager.ConnectionStrings["mysql_localhost"].ConnectionString;
          
            return strReturn;
        }

        public ConnectionState Connect(ref MySqlConnection pCn)
        {
            try
            {
                pCn.Open();
            }
            catch (Exception ex)
            {
                RaiseDbExceptionEvent(this, ex);
            }
            return pCn.State;
        }

        public ConnectionState Disconnect(ref MySqlConnection pCn)
        {
            try
            {
                pCn.Close();
            }
            catch (Exception ex)
            {
                RaiseDbExceptionEvent(this, ex);
            }
            return pCn.State;
        }

        public DataTable DBQuery(string strSqlQuery)
        {
            MySqlConnection cn = new MySqlConnection(this.Connection());
            MySqlCommand cm = new MySqlCommand(strSqlQuery, cn);
            cm.CommandType = CommandType.Text;
            cm.CommandText = strSqlQuery;
            DataTable dt = new DataTable();

            try
            {
                MySqlDataAdapter da = new MySqlDataAdapter(cm);
                da.Fill(dt);
                if (dt.Rows.Count == 0)
                {
                    var exceptionErr = new Exception("No results for query: " + strSqlQuery);
                    throw (exceptionErr);
                }
            }
            catch (Exception ex) {

                // add logging to app dir base
                RaiseDbExceptionEvent(this, ex);
            }
            finally
            {
                if (cn.State == ConnectionState.Open) { this.Disconnect(ref cn); }
                cn.Dispose();
            }
            return dt;
        }
        public List<dynamic> DBQueryDynamic(string strSqlQuery)
        {
            List<dynamic> dynamicObjectList = null;
            DataTable dt = DBQuery(strSqlQuery);
            try
            {
                if (dt.Rows.Count > 0)
                {
                    dynamicObjectList = new List<dynamic>();
                   foreach(DataRow dr in dt.Rows)
                    {
                        dynamic dobj = new  ExpandoObject();
                        foreach (DataColumn dc in dt.Columns)
                        {
                            
                            var dobjDict = dobj as IDictionary<string,object>;
                            if (dobjDict.ContainsKey(dc.ColumnName))
                            {
                                dobjDict[dc.ColumnName] = dr[dc.ColumnName];
                            }
                            else if (!dobjDict.ContainsKey(dc.ColumnName))
                            {
                                dobjDict.Add(dc.ColumnName, dr[dc.ColumnName]);

                            }


                        }
                        dynamicObjectList.Add(dobj);
                    }
                   
                }
                else if (dt.Rows.Count == 0)
                {
                    var exceptionErr = new Exception("Dynamic objects list failed to initialize.  No rows in datatable. "  );
                    throw exceptionErr;
                }
            }
            catch(Exception ex)
            {
                RaiseDbExceptionEvent(this, ex);
            }
           
            return dynamicObjectList;

        }


        public DataTable ScriptedQuery(string filePath)
        {
            return DBQuery(File.ReadAllText(filePath));
        }

        public int DBInt32Scalar(string strSqlQuery)
        {
            MySqlConnection cn = new MySqlConnection(this.Connection());
            this.Connect(ref cn);
            MySqlCommand cm = new MySqlCommand(strSqlQuery, cn);
            cm.CommandType = CommandType.Text;
            int intResult = 0;

            try
            {
                intResult = Convert.ToInt32(cm.ExecuteScalar());
            }
            catch (Exception ex) {
                RaiseDbExceptionEvent(this, ex); }
            finally
            {
                if (cn.State == ConnectionState.Open) { this.Disconnect(ref cn); }
                cn.Dispose();
            }
            return intResult;
        }

        public string DBStringScalar(string strSqlQuery)
        {
            MySqlConnection cn = new MySqlConnection(this.Connection());
            this.Connect(ref cn);
            MySqlCommand cm = new MySqlCommand(strSqlQuery, cn);
            cm.CommandType = CommandType.Text;
            string strResult = "";

            try
            {
                strResult = Convert.ToString(cm.ExecuteScalar());
            }
            catch (Exception ex) {
                RaiseDbExceptionEvent(this, ex); }
            finally
            {
                if (cn.State == ConnectionState.Open) { this.Disconnect(ref cn); }
                cn.Dispose();
            }
            return strResult;
        }
        #region event handlers
        public delegate void DbExceptionEventDelegate(object sender, Exception e);

        public event DbExceptionEventDelegate DbExceptionEventHandler;
        protected virtual void RaiseDbExceptionEvent(object sender, Exception e)
        {
          //  var ErrorEvent = new LibCommon.Logs.LogFileWriter.LogEntry();
            //ErrorEvent.Category = "";
            //ErrorEvent.EventTypeName = "";


            if (DbExceptionEventHandler != null)
            {
                try
                {

                }
                catch (Exception ex)
                {
                    Console.WriteLine("Logto file attempt failed!");
                    Console.WriteLine("Exception Message: " + ex.Message);
                    Console.WriteLine("--------start---------------------------");
                    Console.WriteLine("Original Error: " + e.Message);
                    Console.WriteLine("--------end-----------------------------");
                }
                DbExceptionEventHandler(this, e);
            }
        }
        #endregion
    }
}
