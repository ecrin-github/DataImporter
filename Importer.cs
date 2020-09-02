
using System;

namespace DataImporter
{
	public class Importer
	{
		public void Import(DataLayer repo, LoggingDataLayer logging_repo, bool build_tables)
		{
			string connstring = repo.ConnString;
			Source source = repo.Source;
			
			// proceed if both required parameters are valid
			if (source.id > 0)
			{
				if (build_tables)
				{
					ADBuilder adb = new ADBuilder(connstring, source);
					adb.DeleteADStudyTables();
					adb.DeleteADObjectTables();
					adb.BuildNewADStudyTables();
					adb.BuildNewADObjectTables();
					Helpers.SendMessage("Rebuilt AD tables");
    			}

				// create temporary tables to hold new, edited, deleted 
				// studies and data objects (data objects lists are redundant
				// act as a check mechanism)
				TempTableCreator table_creator = new TempTableCreator(connstring);
				if (source.has_study_tables)
				{
					table_creator.CreateTempStudiesTable();
					Helpers.SendMessage("Created temp studies table");
				}
				table_creator.CreateTempDataObjectsTable();
				Helpers.SendMessage("Created temp objects table");

				// Populate the tables with the ids of new, matched, edited and deleted
				// studies and data objects. (N.B. Some sources are data objects only)

				TempTableFiller filler = new TempTableFiller(connstring);
				if (source.has_study_tables)
				{
					filler.IdentifyNewStudies();
					filler.IdentifyIdenticalStudies();
					filler.IdentifyEditedStudies();
					filler.IdentifyDeletedStudies();
					Helpers.SendMessage("Filled temp studies table");
				}

				filler.IdentifyNewDataObjects();
				filler.IdentifyIdenticalDataObjects();
				filler.IdentifyEditedDataObjects();
				filler.IdentifyDeletedDataObjects();
				Helpers.SendMessage("Filled temp objects table");

				// Create import event log record
				int import_id = logging_repo.GetNextImportEventId();
				bool count_deleted = logging_repo.CheckIfFullHarvest(source.id);
				ImportEvent import = filler.CreateImportEvent(import_id, source, count_deleted);

				// set up sf monitor tables as foreign tables, temporarily

				DataTransferrer transferrer = new DataTransferrer(connstring, source);
				transferrer.EstablishForeignMonTables(repo.User_Name, repo.Password);

				// for studies with status 1, (= new) add these, their attributes,
				// their data objects, and their 

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
				Helpers.SendMessage("Full hash values updated");

				logging_repo.StoreImportEvent(import);

				// Remove temp tables and foreign tables

				transferrer.DropForeignMonTables();
				TempTableDropper temp_dropper = new TempTableDropper(connstring);
				temp_dropper.DeleteTempStudiesTable();
				temp_dropper.TempDataObjectsTable();
			}
		}
	}
}
