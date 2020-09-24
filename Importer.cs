
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
				CatalogueTableCreator ctc = new CatalogueTableCreator(connstring);
				if (source.has_study_tables)
				{
					ctc.CreateStudiesCatTable();
					ctc.CreateStudyChangedAttsTable();
					Helpers.SendMessage("Created studies catalogue tables");
				}
				ctc.CreateDataObjectsCatTable();
				ctc.CreateDataObjectsChangedAttsTable();
				Helpers.SendMessage("Created data objects catalogue tables");

				// Populate the tables with the ids of new, matched, edited and deleted
				// studies and data objects. (N.B. Some sources are data objects only)

				CatalogueTableFiller filler = new CatalogueTableFiller(connstring);
				if (source.has_study_tables)
				{
					filler.IdentifyNewStudies();
					filler.IdentifyIdenticalStudies();
					filler.IdentifyEditedStudies();
					filler.IdentifyDeletedStudies();
					filler.IdentifyChangedStudyRecs();
					filler.IdentifyChangedStudyAtts();
					Helpers.SendMessage("Filled studies catalogue tables");
				}

				// need to add attributes

				filler.IdentifyNewDataObjects();
				filler.IdentifyIdenticalDataObjects();
				filler.IdentifyEditedDataObjects();
				filler.IdentifyDeletedDataObjects();
				filler.IdentifyChangedObjectRecs();
				if (source.has_dataset_properties)
				{
					filler.IdentifyChangedObjectAtts();
				}
				Helpers.SendMessage("Filled data objects catalogue tables");

				// need to add attributes

				// Create import event log record
				int import_id = logging_repo.GetNextImportEventId();
				bool count_deleted = logging_repo.CheckIfFullHarvest(source.id);
				ImportEvent import = filler.CreateImportEvent(import_id, source, count_deleted);

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
				Helpers.SendMessage("Full hash values updated");


				// Remove foreign tables and store import event details

				transferrer.DropForeignMonTables();
				logging_repo.StoreImportEvent(import);

			}
		}
	}
}
