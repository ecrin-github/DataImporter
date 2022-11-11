

namespace DataImporter
{
    class ImportBuilder
    {
        ISource _source;
        IMonitorDataLayer _logging_repo;
        LoggingHelper _logger;
        ImportTableCreator itc;
        ImportTableManager itm;
        string _connstring;

        public ImportBuilder(ISource source, IMonitorDataLayer logging_repo, LoggingHelper logger)
        {
            _source = source;
            _connstring = _source.db_conn;
            _logging_repo = logging_repo;
            _logger = logger;

            itc = new ImportTableCreator(_connstring);
            itm = new ImportTableManager(_connstring);
        }

        public void CreateImportTables()
        {
            if (_source.has_study_tables)
            {
                itc.CreateStudyRecsToADTable();
                itc.CreateStudyAttsToADTable();
                _logger.LogLine("Created studies to_ad tables");
            }
            itc.CreateObjectRecsToADTable();
            itc.CreateObjectAttsToADTable();
            _logger.LogLine("Created data objects to_ad tables");
        }


        public void FillImportTables(bool count_deleted)
        {
            if (_source.has_study_tables)
            {
                itm.IdentifyNewStudies();
                itm.IdentifyIdenticalStudies();
                itm.IdentifyEditedStudies();
                if (count_deleted) itm.IdentifyDeletedStudies();
                itm.IdentifyChangedStudyRecs();
            }

            _logger.LogLine("Filled studies to_ad table");

            itm.IdentifyNewDataObjects();
            itm.IdentifyIdenticalDataObjects();
            itm.IdentifyEditedDataObjects();
            if (count_deleted) itm.IdentifyDeletedDataObjects();
            itm.IdentifyChangedObjectRecs();
            if (_source.has_object_datasets) itm.IdentifyChangedDatasetRecs();

            _logger.LogLine("Filled data objects to_ad table");

            if (_source.has_study_tables)
            {
                itm.SetUpTempStudyAttSets();
                itm.IdentifyChangedStudyAtts();
                itm.IdentifyNewStudyAtts();
                if (count_deleted) itm.IdentifyDeletedStudyAtts();
            }

            _logger.LogLine("Filled study atts table");

            itm.SetUpTempObjectAttSets();
            itm.IdentifyChangedObjectAtts();
            itm.IdentifyNewObjectAtts();
            if (count_deleted) itm.IdentifyDeletedObjectAtts();
            itm.DropTempAttSets();
            _logger.LogLine("Filled data objects atts table");
        }

        public ImportEvent CreateImportEvent(int import_id)
        {
            return itm.CreateImportEvent(import_id, _source);
        }
    }
}
