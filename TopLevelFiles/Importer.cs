using Serilog;
using System;

namespace DataImporter
{
    public class Importer : IImporter
    {

        ILogger _logger;
        ILoggerHelper _logger_helper;
        IMonitorDataLayer _mon_repo;
        ITestingDataLayer _test_repo;

        public Importer(ILogger logger, ILoggerHelper logger_helper,
                         IMonitorDataLayer mon_repo, ITestingDataLayer test_repo)
        {
            _logger = logger;
            _logger_helper = logger_helper;
            _mon_repo = mon_repo;
            _test_repo = test_repo;
        }

        public int Run(Options opts)

        {
            try
            {
                _logger_helper.LogHeader("STARTING IMPORTER");
                _logger_helper.LogCommandLineParameters(opts);

                if (!opts.using_test_data && !opts.create_test_report_only)
                {
                    // Simply import the data for each listed source.

                    foreach (int source_id in opts.source_ids)
                    {
                        ImportData(source_id, opts.rebuild_ad_tables, false);
                    }
                }
                else
                {
                    if (!opts.create_test_report_only)
                    {
                        // first recreate the ADcomposite tables.

                        _test_repo.SetUpADCompositeTables();

                        // Go through and import each test source.

                        foreach (int source_id in opts.source_ids)
                        {
                            ImportData(source_id, true, true);
                            _logger_helper.LogHeader("ENDING " + source_id.ToString() + " first test pass");
                        }

                        // make scripted changes to the ad tables to
                        // create diffs between them
                        _test_repo.ApplyScriptedADChanges();

                        // Go through each test source again,
                        // this time keeping the ad tables.

                        foreach (int source_id in opts.source_ids)
                        {
                            ImportData(source_id, false, true);
                            _logger_helper.LogHeader("ENDING " + source_id.ToString() + " second test pass");
                        }

                        // construct a log detailing differences between the
                        // expacted and actual (composite ad) values.
                    }

                    _test_repo.ConstructDiffReport();
                }
                

                _logger_helper.LogHeader("Closing Log");
                return 0;
            }

            catch (Exception e)
            {
                _logger.Error(e.Message);
                _logger.Error(e.StackTrace);
                _logger_helper.LogHeader("Closing Log");
                return -1;
            }
        }


        private void ImportData(int source_id, bool rebuild_ad_tables, bool using_test_data)
        {
            // Obtain source details, augment with connection string for this database.

            ISource source = _mon_repo.FetchSourceParameters(source_id);
            Credentials creds = _mon_repo.Credentials;
            source.db_conn = creds.GetConnectionString(source.database_name, using_test_data);

            _logger_helper.LogStudyHeader(using_test_data, "For source: " + source.id + ": " + source.database_name);
            _logger_helper.LogHeader("Setup");

            if (using_test_data)
            {
                // first need to copy sd data back from composite
                // sd tables to the sd tables...

                _test_repo.RetrieveSDData(source);
            }

            // Establish top level builder classes and 
            // set up sf monitor tables as foreign tables, temporarily.

            ImportBuilder ib = new ImportBuilder(source, _mon_repo, _logger);
            DataTransferrer transferrer = new DataTransferrer(source, _logger);
            transferrer.EstablishForeignMonTables(creds);
            _logger.Information("Foreign (mon) tables established in database");

            // Recreate ad tables if necessary. If the second pass of a 
            // test loop will need to retrieve the ad data back from compad

            if (rebuild_ad_tables)
            {
                ADBuilder adb = new ADBuilder(source, _mon_repo, _logger);
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

            _logger_helper.LogHeader("Start Import Process");
            _logger_helper.LogHeader("Create and fill diff tables");
            ib.CreateImportTables();
            bool count_deleted = false;
            if (!using_test_data)
            {
                count_deleted = _mon_repo.CheckIfFullHarvest(source.id);
            }
            ib.FillImportTables(count_deleted);
            _mon_repo.LogDiffs(source);


            // Create import event log record and start 
            // the data transfer proper...

            int import_id = _mon_repo.GetNextImportEventId();
            ImportEvent import = ib.CreateImportEvent(import_id);

            // Consider new studies, record dates, edited studies and / or objects,
            // and any deleted studies / objects

            _logger_helper.LogHeader("Adding new data");
            if (source.has_study_tables)
            {
                transferrer.AddNewStudies(import_id);
            }
            transferrer.AddNewDataObjects(import_id);


            _logger_helper.LogHeader("Updating dates of data");
            transferrer.UpdateDatesOfData();


            _logger_helper.LogHeader("Editing existing data where necessary");
            if (source.has_study_tables)
            {
                transferrer.UpdateEditedStudyData(import_id);
            }
            transferrer.UpdateEditedDataObjectData(import_id);


            _logger_helper.LogHeader("Deleting data no longer present in source");
            if (source.has_study_tables)
            {
                transferrer.RemoveDeletedStudyData(import_id);
            }
            transferrer.RemoveDeletedDataObjectData(import_id);


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


            // Ensure that the full hash records have been updated
            // may not have been if change was only in attribute(s).
            // Remove foreign tables 

            _logger_helper.LogHeader("Tidy up and finish");
            transferrer.UpdateFullStudyHashes();
            transferrer.DropForeignMonTables();
            _logger.Information("Foreign (mon) tables removed from database");

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
