using Dapper;
using Npgsql;
using Serilog;

namespace DataImporter
{
    class TestReportBuilder
    {
        private ILogger _logger;
        ILoggerHelper _logger_helper;
        private string _db_conn;


        public TestReportBuilder(string db_conn, ILogger logger, ILoggerHelper logger_helper)
        {
            _db_conn = db_conn;
            _logger = logger;
            _logger_helper = logger_helper;
        }


        public void CompareRecordCounts()
        {
            _logger.Information("");
        }


        public void CompareFullHashes()
        {

        }


        public void CompareCompositeHashes()
        {

        }


        public void CompareRecordHashes()
        {

        }


        public void CompareFields()
        {

        }


        //private 



    }
}
