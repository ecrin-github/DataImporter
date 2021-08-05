using System;
using System.Collections.Generic;
using System.Text;

namespace DataImporter
{
    public interface ILoggerHelper
    {
        void LogCommandLineParameters(Options opts);
        void LogHeader(string header_text);
        void LogStudyHeader(bool using_test_data, string dbline);

        void LogTableStatistics(ISource s, string schema);

        string GetTableRecordCount(string db_conn, string schema, string table_name);
        IEnumerable<hash_stat> GetHashStats(string db_conn, string schema, string table_name);
    }
}
