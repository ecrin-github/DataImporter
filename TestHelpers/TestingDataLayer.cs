using Dapper;
using Npgsql;
using Serilog;
using System.Collections.Generic;


namespace DataImporter
{
    public class TestingDataLayer : ITestingDataLayer
    {
        ICredentials _credentials;
        NpgsqlConnectionStringBuilder builder;
        private string _db_conn;
        ILogger _logger;
        ILoggerHelper _logger_helper;

        public TestingDataLayer(ILogger logger, ICredentials credentials, ILoggerHelper logger_helper)
        {
            builder = new NpgsqlConnectionStringBuilder();

            builder.Host = credentials.Host;
            builder.Username = credentials.Username;
            builder.Password = credentials.Password;

            builder.Database = "test";
            _db_conn = builder.ConnectionString;

            _credentials = credentials;

            _logger = logger;
            _logger_helper = logger_helper;
        }

        public Credentials Credentials => (Credentials)_credentials;

        
        public IEnumerable<int> ObtainTestSourceIDs()
        {
            string sql_string = @"select distinct source_id 
                                 from expected.source_studies;";

            using (var conn = new NpgsqlConnection(_db_conn))
            {
                return conn.Query<int>(sql_string);
            }
        }


        public void SetUpADCompositeTables()
        {
            ADCompTableBuilder atb = new ADCompTableBuilder(_db_conn);
            atb.BuildStudyTables();
            atb.BuildObjectTables();
            _logger.Information("Composite AD tables established");
        }

       

        public void RetrieveSDData(ISource source)
        {
            RetrieveSDDataBuilder rsdb = new RetrieveSDDataBuilder(source);
            rsdb.DeleteExistingSDStudyData();
            rsdb.DeleteExistingSDObjectData();
            rsdb.RetrieveStudyData();
            rsdb.RetrieveObjectData();
            _logger.Information("SD test data for source " + source.id + " retrieved from CompSD");
        }


        public void RetrieveADData(ISource source)
        {
            RetrieveADDataBuilder radb = new RetrieveADDataBuilder(source);
            radb.DeleteExistingADStudyData();
            radb.DeleteExistingADObjectData();
            radb.RetrieveStudyData();
            radb.RetrieveObjectData();
            _logger.Information("AD test data for source " + source.id + " retrieved from CompAD");
        }


        public void TransferADDataToComp(ISource source)
        {
            TransferADDataBuilder tdb = new TransferADDataBuilder(source);
            tdb.DeleteExistingStudyData();
            tdb.DeleteExistingObjectData();
            _logger.Information("Any existing AD test data for source " + source.id + " removed from CompAD");
            tdb.TransferStudyData();
            tdb.TransferObjectData();
            _logger.Information("New AD test data for source " + source.id + " added to CompAD");
        }


        public void ApplyScriptedADChanges()
        {
            ADChangesBuilder tdb = new ADChangesBuilder(_db_conn);

            /*
            tdb.DeleteExistingStudyData();
            tdb.DeleteExistingObjectData();
            _logger.Information("Any existing SD test data for source " + source.id + " removed from CompSD");

            tdb.TransferStudyData();
            tdb.TransferObjectData();
            _logger.Information("New SD test data for source " + source.id + " added to CompSD");
            */
        }


        public void ConstructDiffReport()
        {
            TestReportBuilder tdb = new TestReportBuilder(_db_conn);

            if (tdb.CompareStudyRecordCounts())
            {
                tdb.CompareStudyRecords();
                tdb.CompareStudyAttributes();
                tdb.CompareStudyHashes();
            }

            if (tdb.CompareObjectRecordCounts())
            {
                tdb.CompareObjectRecords();
                tdb.CompareObjectAttributes();
                tdb.CompareObjectHashes();
            }

            tdb.CompareFullHashes();


            tdb.Close();

        }

    }
}