namespace DataImporter
{
    class ImportBuilder
    {
        string connstring;
        Source source;
        ImportTableCreator itc;
        ImportTableManager itm;
        LoggingDataLayer logging_repo;

        public ImportBuilder(string _connstring, Source _source, LoggingDataLayer _logging_repo)
        {
            connstring = _connstring;
            source = _source;
            itc = new ImportTableCreator(connstring);
            itm = new ImportTableManager(connstring);
            logging_repo = _logging_repo;
        }

        public void CreateImportTables()
        {
            if (source.has_study_tables)
            {
                itc.CreateStudyRecsToADTable();
                itc.CreateStudyAttsToADTable();
                logging_repo.LogLine("Created studies to_ad tables");
            }
            itc.CreateObjectRecsToADTable();
            itc.CreateObjectAttsToADTable();
            logging_repo.LogLine("Created data objects to_ad tables");
        }


        public void FillImportTables(bool count_deleted)
        {
            if (source.has_study_tables)
            {
                itm.IdentifyNewStudies();
                itm.IdentifyIdenticalStudies();
                itm.IdentifyEditedStudies();
                if (count_deleted) itm.IdentifyDeletedStudies();
                itm.IdentifyChangedStudyRecs();
            }

            logging_repo.LogLine("Filled studies to_ad table");

            itm.IdentifyNewDataObjects();
            itm.IdentifyIdenticalDataObjects();
            itm.IdentifyEditedDataObjects();
            if (count_deleted) itm.IdentifyDeletedDataObjects();
            itm.IdentifyChangedObjectRecs();
            if (source.has_object_datasets) itm.IdentifyChangedDatasetRecs();
            logging_repo.LogLine("Filled data objects to_ad table");

            if (source.has_study_tables)
            {
                itm.SetUpTempStudyAttSets();
                itm.IdentifyChangedStudyAtts();
                itm.IdentifyNewStudyAtts();
                if (count_deleted) itm.IdentifyDeletedStudyAtts();
            }
            logging_repo.LogLine("Filled study atts table");

            itm.SetUpTempObjectAttSets();
            itm.IdentifyChangedObjectAtts();
            itm.IdentifyNewObjectAtts();
            if (count_deleted) itm.IdentifyDeletedObjectAtts();
            itm.DropTempAttSets();
            logging_repo.LogLine("Filled data objects atts table");
        }

        public ImportEvent CreateImportEvent(int import_id)
        {
            return itm.CreateImportEvent(import_id, source);
        }
    }
}
