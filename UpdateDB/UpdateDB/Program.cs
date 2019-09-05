using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Dynamic;
using System.Data.SqlClient;
using System.Data;
using System.Transactions;
using ServiceStack;
using ServiceStack.Text;
using Dapper;

namespace csvreader
{
    class Program
    {
        static void Main(string[] args)
        {
            //string DBConnectionString = "Data Source=10.16.16.104;Initial Catalog=HMeter;User ID=yutest;Password=116yvmp!@#$;";
            string DBConnectionString = "Data Source=10.16.16.62;Initial Catalog=HVCS_SYS;User ID=mds;Password=Mds#A23s58~;";
            List<string[]> rows = new List<string[]>();
            var csvfile = File.ReadAllText("D:\\UVA\\Practice\\csvfile reader\\是否活戶及註冊欄位.csv");

            /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            //版本2
            DataTable dt = new DataTable();
            dt.Columns.Add("CUST_NO", typeof(string));
            dt.Columns.Add("YYYYMM", typeof(string));
            dt.Columns.Add("IsAlive", typeof(string));
            dt.Columns.Add("IsRegister", typeof(string));

            int i = 1;
            string sb = "UPDATE [HVCS_SYS].[dbo].[HVCS_COM_QUERY_DAY_DW_] " +
                        "SET [IsAlive] = ut.[IsAlive], [IsRegister] = ut.[IsRegister] " +
                        "FROM [HVCS_SYS].[dbo].[HVCS_COM_QUERY_DAY_DW_] bt " +
                        "JOIN @UpdatedTable ut " +
                        "ON bt.[CUST_NO] = ut.[CUST_NO] " +
                        "AND SUBSTRING(CONVERT(VARCHAR(10),[YYYYMMDD],112),1,6) = ut.[YYYYMM];";

            using (var tx = new TransactionScope())
            {
                //using (SqlConnection sqlConnection = new SqlConnection(DBConnectionString))
                //{
                    foreach (var line in CsvReader.ParseLines(csvfile))
                    {
                        string[] strArray = CsvReader.ParseFields(line).ToArray();

                        //民國年變西元年
                        int int_input = Int32.Parse(strArray[1]) + 191100;
                        string str_input = int_input.ToString();

                        var row = dt.NewRow();
                        row["CUST_NO"] = strArray[0];
                        row["YYYYMM"] = str_input;
                        row["IsAlive"] = strArray[2];
                        row["IsRegister"] = strArray[3];

                        if(i % 200 == 0 || i == 480614)//每兩百筆
                        {
                            dt.Rows.Add(row);
                            Console.WriteLine("第" + i + "開始...");

                            //TEST@_@
                            int qqq = 0;
                            if (i == 480614)
                                qqq = 14;
                            else
                                qqq = 200;
                            for(int qq = 0; qq < qqq; qq++)
                            {
                                Console.WriteLine(dt.Rows[qq][0] + " // " + dt.Rows[qq][1] + " // " + dt.Rows[qq][2] + " // " + dt.Rows[qq][3]);
                            }

                            //Table-Valued Parameters(TVP)需在資料庫先建立'使用者定義資料表類型'
                            //CREATE TYPE [dbo].[_DW_] AS TABLE
                            //  ([CUST_NO] [nvarchar](11) NOT NULL,
                            //   [YYYYMM] [nvarchar](11) NOT NULL,
                            //   [IsAlive] [nvarchar](3) NULL,
                            //   [IsRegister] [nvarchar](3) NULL);
                            //sqlConnection.Execute(sb, new { @UpdatedTable = dt.AsTableValuedParameter("_DW_") });

                            dt.Rows.Clear();
                            Console.WriteLine("第" + i + "結束...");
                        }
                        else
                        {
                            dt.Rows.Add(row);
                        }

                        i++;
                //}
                }
                tx.Complete();
            }

            /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            //版本1
            /*DataTable dt = new DataTable();
            dt.Columns.Add("CUST_NO", typeof(string));
            dt.Columns.Add("YYYYMM", typeof(string));
            dt.Columns.Add("IsAlive", typeof(string));
            dt.Columns.Add("IsRegister", typeof(string));

            foreach (var line in CsvReader.ParseLines(csvfile))
            {
                string[] strArray = CsvReader.ParseFields(line).ToArray();

                //民國年變西元年
                int int_input = Int32.Parse(strArray[1]) + 191100;
                string str_input = int_input.ToString();

                var row = dt.NewRow();
                row["CUST_NO"] = strArray[0];
                row["YYYYMM"] = str_input;
                row["IsAlive"] = strArray[2];
                row["IsRegister"] = strArray[3];

                dt.Rows.Add(row);
            }

            string sb = $"UPDATE [HVCS_SYS].[dbo].[HVCS_COM_QUERY_DAY_DW_] " +
                        $"SET [IsAlive] = ut.[IsAlive], [IsRegister] = ut.[IsRegister] " +
                        $"FROM [HVCS_SYS].[dbo].[HVCS_COM_QUERY_DAY_DW_] bt " +
                        $"JOIN @UpdatedTable ut " +
                        $"ON bt.[CUST_NO] = ut.[CUST_NO] " +
                        $"AND SUBSTRING(CONVERT(VARCHAR(10),[YYYYMMDD],112),1,6) = ut.[YYYYMM];";

            using (var tx = new TransactionScope())
            {
                using (SqlConnection sqlConnection = new SqlConnection(DBConnectionString))
                {
                    //Table-Valued Parameters(TVP)需在資料庫先建立'使用者定義資料表類型'
                    //CREATE TYPE [dbo].[_DW_] AS TABLE
                    //  ([CUST_NO] [nvarchar](11) NOT NULL,
                    //   [YYYYMM] [nvarchar](11) NOT NULL,
                    //   [IsAlive] [nvarchar](3) NULL,
                    //   [IsRegister] [nvarchar](3) NULL);
                    sqlConnection.Execute(sb, new { @UpdatedTable = dt.AsTableValuedParameter("_DW_") });
                }
                tx.Complete();
            }*/

                //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                //版本0
                /*string Cnnstr = "Data Source=10.16.16.62;Initial Catalog=HVCS_SYS;User ID=mds;Password=Mds#A23s58~;";

                DataTable DT = new DataTable();
                DT.Columns.Add("CUST_NO");
                DT.Columns.Add("YYYYMMDD");
                DT.Columns.Add("IsAlive");
                DT.Columns.Add("IsRegister");

                var csvfile = File.ReadAllText("D:\\是否活戶及註冊欄位123.csv");
                List<string[]> rows = new List<string[]>();

                foreach (var line in CsvReader.ParseLines(csvfile))
                {
                    string[] strArray = CsvReader.ParseFields(line).ToArray();
                    rows.Add(strArray);
                }

                foreach (int i in Enumerable.Range(0, rows.Count()))
                {
                    DT.Rows.Add(new object[] { rows[i][2], rows[i][3] });
                    //Console.WriteLine(rows[i][0]);
                    //Console.WriteLine(rows[i][1]);
                    //Console.WriteLine(rows[i][2]);
                    //Console.WriteLine(rows[i][3]);
                }
                string[] PK = new string[] { "CUST_NO", "YYYYMMDD" };
                BatchUpdate(Cnnstr, DT, "HVCS_COM_QUERY_DAY_DW_", PK);*/

                /*public static string BatchUpdate(string connstr, DataTable TargetTable, string TableName, string[] Primarykeys)
                {
                    string ProcedureName = "TVPBatchUpdate";
                    string sqlstr = "";
                    string PKtemp = "";
                    string JoinKey = "";
                    string Result = "";
                    try
                    {
                        DateTime stratTime = DateTime.Now;
                        #region 建立SQL自訂TVP類型
                        sqlstr = " CREATE TYPE [dbo].[" + ProcedureName + "] AS TABLE ";
                        sqlstr += Environment.NewLine + " (  ";
                        //注意! 作為PK值/Index的欄位不可以開(MAX)
                        foreach (string Primarykey in Primarykeys)
                        {
                            sqlstr += Environment.NewLine + "    [" + Primarykey + "] [NVARCHAR](1000) NOT NULL ,";
                            PKtemp += ",[" + Primarykey + "]";
                            JoinKey += "AND [bt].[" + Primarykey + "] = [ut].[" + Primarykey + "]";
                        }
                        PKtemp = PKtemp.TrimStart(new char[] { ',' });
                        JoinKey = JoinKey.TrimStart(new char[] { 'A', 'N', 'D' });
                        foreach (DataColumn DC in TargetTable.Columns)
                        {
                            if (Primarykeys.Any(s => DC.ColumnName.Contains(s)))
                            {
                                continue;
                            }
                            sqlstr += Environment.NewLine + "    [" + DC.ColumnName + "] [NVARCHAR](MAX) NULL ,";
                        }
                        sqlstr = sqlstr.TrimEnd(new char[] { ',' });
                        if (Primarykeys.Length > 0)
                        {
                            sqlstr += Environment.NewLine + "  , PRIMARY KEY(";
                            sqlstr += PKtemp;
                            sqlstr += Environment.NewLine + " ) ";
                        }
                        sqlstr += Environment.NewLine + " ); ";
                        #endregion
                        using (SqlConnection scnn = new SqlConnection(connstr))
                        {
                            scnn.Open();
                            //宣告SqlBulkCopy
                            using (SqlCommand cmd = new SqlCommand(sqlstr, scnn))
                            {
                                cmd.CommandType = CommandType.Text;
                                cmd.CommandTimeout = 90;
                                cmd.ExecuteNonQuery();
                            }
                            scnn.Close();
                            scnn.Dispose();
                        }
                        #region 執行更新語法
                        sqlstr = " UPDATE [dbo].[" + TableName + "] ";
                        sqlstr += Environment.NewLine + "    SET ";
                        foreach (DataColumn DC in TargetTable.Columns)
                        {
                            sqlstr += Environment.NewLine + "    [" + DC.ColumnName + "] = [ut].[" + DC.ColumnName + "] ,";
                        }
                        sqlstr = sqlstr.TrimEnd(new char[] { ',' });
                        sqlstr += Environment.NewLine + " FROM [dbo].[" + TableName + "] [bt] ";
                        sqlstr += Environment.NewLine + "      JOIN @UpdatedTable [ut] ON " + JoinKey;
                        #endregion
                        using (SqlConnection scnn = new SqlConnection(connstr))
                        {
                            scnn.Open();
                            using (SqlCommand cmd = new SqlCommand(sqlstr, scnn))
                            {
                                cmd.CommandType = CommandType.Text;
                                cmd.CommandTimeout = 90;
                                SqlParameter param = new SqlParameter("@UpdatedTable", SqlDbType.Structured);
                                param.Value = TargetTable;
                                param.TypeName = ProcedureName;
                                cmd.Parameters.Add(param);
                                cmd.ExecuteNonQuery();
                            }
                            scnn.Close();
                            scnn.Dispose();
                        }
                        DateTime EndTime = DateTime.Now;
                        TimeSpan DeltaTime = EndTime - stratTime;
                        Result = DeltaTime.Hours + ":" + DeltaTime.Minutes + ":" + DeltaTime.Seconds + "." + DeltaTime.Milliseconds;
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                    }
                    finally
                    {
                        try
                        {
                            sqlstr = "DROP TYPE [dbo].[" + ProcedureName + "]";
                            using (SqlConnection scnn = new SqlConnection(connstr))
                            {
                                scnn.Open();
                                //宣告SqlBulkCopy
                                using (SqlCommand cmd = new SqlCommand(sqlstr, scnn))
                                {
                                    cmd.CommandType = CommandType.Text;
                                    cmd.CommandTimeout = 90;
                                    cmd.ExecuteNonQuery();
                                }
                                scnn.Close();
                                scnn.Dispose();
                            }
                        }
                        catch
                        {
                            //如果TVP已經刪除 那就不作處理
                        }
                    }
                    return Result;
                }*/
        }
    }
}