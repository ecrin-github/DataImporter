
using System;

namespace DataImporter
{
	public class Importer
	{
		public void Import(int source_id, DataLayer repo, LoggingDataLayer logging_repo, bool build_tables)
		{
			string connstring = repo.ConnString;
			Source source = repo.Source;

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
				StringHelpers.SendFeedback("Rebuilt AD tables");

				HistoryTableCreator htc = new HistoryTableCreator(connstring);
				htc.CreateHistoryMasterTable();
				if (source.has_study_tables)
				{
					htc.CreateStudyRecsHistoryTable();
					htc.CreateStudyAttsHistoryTable();
				}
				htc.CreateObjectRecsHistoryTable();
				htc.CreateObjectAttsHistoryTable();
				StringHelpers.SendFeedback("created import history tables");
			}

			// create temporary tables to hold new, edited, deleted 
			// studies and data objects (data objects lists are redundant
			// act as a check mechanism)
			ImportTableCreator itc = new ImportTableCreator(connstring);
			if (source.has_study_tables)
			{
				itc.CreateStudyRecsImportTable();
				itc.CreateStudyAttsImportTable();
				StringHelpers.SendFeedback("Created studies catalogue tables");
			}
			itc.CreateObjectRecsImportTable();
			itc.CreateObjectAttsImportTable();
			StringHelpers.SendFeedback("Created data objects catalogue tables");

			// Populate the tables with the ids of new, matched, edited and deleted
			// studies and data objects. (N.B. Some sources are data objects only)

			ImportTableManager itm = new ImportTableManager(connstring);
			if (source.has_study_tables)
			{
				itm.IdentifyNewStudies();
				itm.IdentifyIdenticalStudies();
				itm.IdentifyEditedStudies();
				itm.IdentifyDeletedStudies();
				itm.IdentifyChangedStudyRecs();
				itm.IdentifyChangedStudyAtts();
				StringHelpers.SendFeedback("Filled studies import tables");
			}

			itm.IdentifyNewDataObjects();
			itm.IdentifyIdenticalDataObjects();
			itm.IdentifyEditedDataObjects();
			itm.IdentifyDeletedDataObjects();
			itm.IdentifyChangedObjectRecs();
			if (source.has_dataset_properties)
			{
				itm.IdentifyChangedObjectAtts();
			}
			StringHelpers.SendFeedback("Filled data objects import tables");

			// need to add attributes

			// Create import event log record
			int import_id = logging_repo.GetNextImportEventId();
			bool count_deleted = logging_repo.CheckIfFullHarvest(source.id);
			ImportEvent import = itm.CreateImportEvent(import_id, source, count_deleted);

			// Start the data transfer proper...

			DataTransferrer transferrer = new DataTransferrer(connstring, source);

			// set up sf monitor tables as foreign tables, temporarily
			transferrer.EstablishForeignMonTables(repo.User_Name, repo.Password);

			// for studies with status 1, (= new) add these, their attributes,
			// their data objects, and their object attributes

			if (source.has_study_tables)
			{
				transferrer.AddNewStudies(import_id);
			}
			if (import.num_new_objects > 0)
			{
				transferrer.AddNewDataObjects(import_id);
			}

			// update studies and data objects 'data of data' for recently 
			// downloaded data that previously existed (= status of 2 and 3 
			// for both studies and data objects)

			if (source.has_study_tables)
			{
				transferrer.UpdateDateOfStudyData();
			}
			transferrer.UpdateDateOfDataObjectData();


			// then a need to examine the edited data (status = 2)
			if (source.has_study_tables)
			{
				transferrer.UpdateEditedStudyData(import_id);
			}
			transferrer.UpdateEditedDataObjectData(import_id);


			// Finally, remove any deleted studies / objects from the ad tables
			if (source.has_study_tables)
			{
				transferrer.RemoveDeletedStudyData(import_id);
			}
			transferrer.RemoveDeletedDataObjectData(import_id);


			// ensure that the full hash records have been updated
			// may not have been if change was only in attribute(s)

			if (source.has_study_tables)
			{
				transferrer.UpdateFullStudyHash();
			}
			transferrer.UpdateFullObjectHash();
			StringHelpers.SendFeedback("Full hash values updated");


			// Remove foreign tables and store import event details
			// Transfer the import record data to the history tables
			// update the history record class and add it as and record to
			// the histgory_master table

			transferrer.DropForeignMonTables();
			logging_repo.StoreImportEvent(import);
			HistoryTableManager htm = new HistoryTableManager(connstring);
			htm.ObtainMasterListState();    // see if previous imports to consider
			if (source.has_study_tables)
			{
				htm.AddStudyRecsAdded();
				htm.AddStudyRecsDeleted();
				htm.ProcessEditedStudyRecs();
				htm.ProcessEditedStudyAtts();
			}
			htm.AddObjectRecsAdded();
			htm.AddObjectRecsDeleted();
			htm.ProcessEditedObjectRecs();
			htm.ProcessEditedObjectAtts();
			htm.CreateAndStoreHistoryRecord(import);
		}
	}
}
