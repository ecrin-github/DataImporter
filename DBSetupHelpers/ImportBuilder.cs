namespace DataImporter
{
    class ImportBuilder
    {
        string connstring;
        Source source;
        ImportTableCreator itc;
        ImportTableManager itm;

        public ImportBuilder(string _connstring, Source _source)
        {
            connstring = _connstring;
            source = _source;
            itc = new ImportTableCreator(connstring);
            itm = new ImportTableManager(connstring);
        }

        public void CreateImportTables()
        {
            if (source.has_study_tables)
            {
                itc.CreateStudyRecsToADTable();
                itc.CreateStudyAttsToADTable();
                StringHelpers.SendFeedback("Created studies to_ad tables");
            }
            itc.CreateObjectRecsToADTable();
            itc.CreateObjectAttsToADTable();
            StringHelpers.SendFeedback("Created data objects to_ad tables");
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

            StringHelpers.SendFeedback("Filled studies to_ad table");

            itm.IdentifyNewDataObjects();
            itm.IdentifyIdenticalDataObjects();
            itm.IdentifyEditedDataObjects();
            if (count_deleted) itm.IdentifyDeletedDataObjects();
            itm.IdentifyChangedObjectRecs();
            if (source.has_object_datasets) itm.IdentifyChangedDatasetRecs();
            StringHelpers.SendFeedback("Filled data objects to_ad table");

            if (source.has_study_tables)
            {
                itm.SetUpTempStudyAttSets();
                itm.IdentifyChangedStudyAtts();
                itm.IdentifyNewStudyAtts();
                if (count_deleted) itm.IdentifyDeletedStudyAtts();
            }
            StringHelpers.SendFeedback("Filled study atts table");

            itm.SetUpTempObjectAttSets();
            itm.IdentifyChangedObjectAtts();
            itm.IdentifyNewObjectAtts();
            if (count_deleted) itm.IdentifyDeletedObjectAtts();
            itm.DropTempAttSets();
            StringHelpers.SendFeedback("Filled data objects atts table");
        }

        public ImportEvent CreateImportEvent(int import_id)
        {
            return itm.CreateImportEvent(import_id, source);
        }
    }
}
