
using System;

namespace DataImporter
{
    public class Importer : IImporter
    {
        LoggingHelper _logging_helper;
        IMonitorDataLayer _mon_repo;
        ITestingDataLayer _test_repo;

        public Importer(IMonitorDataLayer mon_repo, ITestingDataLayer test_repo)
        {
            _mon_repo = mon_repo;
            _test_repo = test_repo;
        }

        public int Run(Options opts)

        {
            try
            {
                if (!opts.using_test_data && !opts.create_test_report)
                {
                    // Simply import the data for each listed source.

                    foreach (int source_id in opts.source_ids)
                    {
                        ISource source = _mon_repo.FetchSourceParameters(source_id);

                        _logging_helper = new LoggingHelper(source.database_name);
                        _logging_helper.LogHeader("STARTING IMPORTER");
                        _logging_helper.LogCommandLineParameters(opts);

                        ImportData(source, opts.rebuild_ad_tables, false, _logging_helper);
                    }
                }
                else
                {
                    // one or both of -F, -G have been used
                    // 'F' = is a test, If present, operates on the sd / ad tables in the test database
                    // 'G' = test report, If present, compares and reports on adcomp and expected tables but does not recreate those tables

                    _logging_helper = new LoggingHelper("test");

                    if (opts.using_test_data)
                    {
                        // first recreate the ADcomposite tables.

                        _test_repo.SetUpADCompositeTables();

                        // Go through and import each test source.

                        foreach (int source_id in opts.source_ids)
                        {
                            ISource source = _mon_repo.FetchSourceParameters(source_id);
                            ImportData(source, true, true, _logging_helper);
                            _logging_helper.LogHeader("ENDING " + source_id.ToString() + ": " + source.database_name + " first test pass");
                        }

                        // make scripted changes to the ad tables to
                        // create diffs between them
                        _test_repo.ApplyScriptedADChanges();

                        // Go through each test source again,
                        // this time keeping the ad tables.

                        foreach (int source_id in opts.source_ids)
                        {
                            ISource source = _mon_repo.FetchSourceParameters(source_id);
                            ImportData(source, false, true, _logging_helper);
                            _logging_helper.LogHeader("ENDING " + source_id.ToString() + ": " + source.database_name + " second test pass");
                        }
                    }


                    if (opts.create_test_report)
                    {
                        // construct a log detailing differences between the
                        // expacted and actual (composite ad) values.

                        _test_repo.ConstructDiffReport();
                    }
                }

                _logging_helper.CloseLog();
                return 0;
            }

            catch (Exception e)
            {
                _logging_helper.LogHeader("UNHANDLED EXCEPTION");
                _logging_helper.LogCodeError("Importer application aborted", e.Message, e.StackTrace);
                _logging_helper.CloseLog();
                return -1;
            }
        }


        private void ImportData(ISource source, bool rebuild_ad_tables, bool using_test_data, LoggingHelper logging_helper)
        {
            // Obtain source details, augment with connection string for this database.

            Credentials creds = _mon_repo.Credentials;
            source.db_conn = creds.GetConnectionString(source.database_name, using_test_data);

            logging_helper.LogStudyHeader(using_test_data, "For source: " + source.id + ": " + source.database_name);
            logging_helper.LogHeader("Setup");

            if (using_test_data)
            {
                // first need to copy sd data back from composite
                // sd tables to the sd tables...

                _test_repo.RetrieveSDData(source);
            }

            // Establish top level builder classes and 
            // set up sf monitor tables as foreign tables, temporarily.

            ImportBuilder ib = new ImportBuilder(source, _mon_repo, logging_helper);
            DataTransferrer transferrer = new DataTransferrer(source, logging_helper);
            transferrer.EstablishForeignMonTables(creds);
            logging_helper.LogLine("Foreign (mon) tables established in database");

            // Recreate ad tables if necessary. If the second pass of a 
            // test loop will need to retrieve the ad data back from compad

            if (rebuild_ad_tables)
            {
                ADBuilder adb = new ADBuilder(source, _mon_repo, logging_helper);
                adb.BuildNewADTables();
            }
            else
            {
                if (using_test_data)
                {
                    _test_repo.RetrieveADData(source);
                }
            }

            // create and fill temporary tables to hold ids and edit statuses  
            // of new, edited, deleted tudies and data objects.

            logging_helper.LogHeader("Start Import Process");
            logging_helper.LogHeader("Create and fill diff tables");
            ib.CreateImportTables();
            bool count_deleted = false;
            if (!using_test_data)
            {
                count_deleted = _mon_repo.CheckIfFullHarvest(source.id);
            }
            ib.FillImportTables(count_deleted);
            logging_helper.LogDiffs(source);


            // Create import event log record and start 
            // the data transfer proper...

            int import_id = _mon_repo.GetNextImportEventId();
            ImportEvent import = ib.CreateImportEvent(import_id);

            // Consider new studies, record dates, edited studies and / or objects,
            // and any deleted studies / objects

            logging_helper.LogHeader("Adding new data");
            if (source.has_study_tables)
            {
                transferrer.AddNewStudies(import_id);
            }
            transferrer.AddNewDataObjects(import_id);


            logging_helper.LogHeader("Updating dates of data");
            transferrer.UpdateDatesOfData();


            logging_helper.LogHeader("Editing existing data where necessary");
            if (source.has_study_tables)
            {
                transferrer.UpdateEditedStudyData(import_id);
            }
            transferrer.UpdateEditedDataObjectData(import_id);


            logging_helper.LogHeader("Deleting data no longer present in source");
            if (source.has_study_tables)
            {
                transferrer.RemoveDeletedStudyData(import_id);
            }
            transferrer.RemoveDeletedDataObjectData(import_id);

            // Ensure that the full hash records have been updated
            // may not have been if change was only in attribute(s).
            // Remove foreign tables 

            logging_helper.LogHeader("Tidy up and finish");
            transferrer.UpdateFullRecordHashes();

            // Update the 'date imported' record in the mon.source data tables
            // Affects all records with status 1, 2 or 3 (non-test imports only)

            if (!using_test_data)
            {
                if (source.has_study_tables)
                {
                    transferrer.UpdateStudiesLastImportedDate(import_id);
                }
                else
                {
                    // only do the objects table if there are no studies (e.g. PubMed)
                    transferrer.UpdateObjectsLastImportedDate(import_id);
                }
            }

            transferrer.DropForeignMonTables();
            logging_helper.LogLine("Foreign (mon) tables removed from database");

            if (using_test_data)
            {
                // copy ad data from ad tables to the compad tables...

                _test_repo.TransferADDataToComp(source);

            }
            else
            {
                // Only store import event for non-test imports.

                _mon_repo.StoreImportEvent(import);
            }

        } 
    }
}
