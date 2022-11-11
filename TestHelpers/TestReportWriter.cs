using Dapper;
using Dapper.Contrib.Extensions;
using Microsoft.Extensions.Configuration;
using Npgsql;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace DataImporter
{
    public class TestReportWriter
    {
        private string logfile_path;
        private StreamWriter sw;

 

        public void OpenLogFile(string logfile_startofpath)
        {
            string dt_string = DateTime.Now.ToString("s", 
                               System.Globalization.CultureInfo.InvariantCulture)
                              .Replace(":", "").Replace("T", " ");
            logfile_path = logfile_startofpath + "IMPORT TEST REPORT";
            logfile_path += " " + dt_string + ".log";
            sw = new StreamWriter(logfile_path, false, System.Text.Encoding.UTF8);

        }

        public void LogLine(string message, string identifier = "")
        {
            string dt_string = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
            string feedback = dt_string + message + identifier;
            Transmit(feedback);
        }


        public void LogSimpleLine(string message, string identifier = "")
        {
            string feedback = message + identifier;
            Transmit(feedback);
        }


        public void BlankLine()
        {
            Transmit("");
        }

        public void LogHeader(string message)
        {
            string dt_string = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
            string header = dt_string + "**** " + message + " ****";
            Transmit("");
            Transmit(header);
        }

        public void LogError(string message, string identifier = "")
        {
            string dt_string = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
            string error_message = dt_string + "***ERROR*** " + message;
            Transmit("");
            Transmit("+++++++++++++++++++++++++++++++++++++++");
            Transmit(error_message);
            Transmit("+++++++++++++++++++++++++++++++++++++++");
            Transmit("");
        }

        public void CloseLog()
        {
            LogHeader("Closing Log");
            sw.Flush();
            sw.Close();
        }


        private void Transmit(string message)
        {
            sw.WriteLine(message);
            Console.WriteLine(message);
        }

        
    }

}
