namespace DataImporter
{
    public class Importer
    {
        public void Import(int source_id, DataLayer repo, LoggingDataLayer logging_repo, bool build_tables)
        {
            string connstring = repo.ConnString;
            Source source = repo.Source;
            //HistoryBuilder hb = new HistoryBuilder(connstring, source);
            ImportBuilder ib = new ImportBuilder(connstring, source);
            DataTransferrer transferrer = new DataTransferrer(connstring, source);

            StringHelpers.SendHeader("Setup");
            if (build_tables)
            {
                ADBuilder adb = new ADBuilder(connstring, source);
                if (source.has_study_tables)
                {
                    adb.DeleteADStudyTables();
                    adb.BuildNewADStudyTables();
                }
                adb.DeleteADObjectTables();
                adb.BuildNewADObjectTables();
            }

            StringHelpers.SendHeader("Create and fill diff tables");
            // create and fill temporary tables to hold ids and edit statuses  
            // of new, edited, deleted tudies and data objects
            ib.CreateImportTables();
            bool count_deleted = logging_repo.CheckIfFullHarvest(source.id);
            ib.FillImportTables(count_deleted);

            // Create import event log record
            int import_id = logging_repo.GetNextImportEventId();
            ImportEvent import = ib.CreateImportEvent(import_id);

            // Start the data transfer proper...
            // set up sf monitor tables as foreign tables, temporarily
            transferrer.EstablishForeignMonTables(repo.User_Name, repo.Password);

            StringHelpers.SendHeader("Adding new data");
            // for studies with status 1, (= new) add these, their attributes,
            // their data objects, and their object attributes

            if (source.has_study_tables)
            {
                transferrer.AddNewStudies(import_id);
            }
            transferrer.AddNewDataObjects(import_id);

            // update studies and data objects 'data of data' for recently 
            // downloaded data that previously existed (= status of 2 and 3 
            // for both studies and data objects)

            StringHelpers.SendHeader("Update dates of data");
            transferrer.UpdateDatesOfData();

            // then a need to examine the edited data (status = 2)
            StringHelpers.SendHeader("Editing existing data");
            if (source.has_study_tables)
            {
                transferrer.UpdateEditedStudyData(import_id);
            }
            transferrer.UpdateEditedDataObjectData(import_id);


            // Finally, remove any deleted studies / objects from the ad tables
            StringHelpers.SendHeader("Removing any deleted data");
            if (source.has_study_tables)
            {
                transferrer.RemoveDeletedStudyData(import_id);
            }
            transferrer.RemoveDeletedDataObjectData(import_id);


            // Ensure that the full hash records have been updated
            // may not have been if change was only in attribute(s).
            // Remove foreign tables and store import event details
            // No - Move to aggregation process - Transfer the import record data to the history tables.

            StringHelpers.SendHeader("Tidy up and finish");
            transferrer.UpdateFullStudyHashes();
            transferrer.DropForeignMonTables();
            logging_repo.StoreImportEvent(import);
    
        }
    }
}
