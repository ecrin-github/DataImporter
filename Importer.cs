
using System;

namespace DataImporter
{
	public class Importer
	{
		public void Import(DataLayer repo, bool build_tables)
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
					filler.UpdateFullStudyHash();
					Helpers.SendMessage("Filled temp studies table");
				}

				filler.IdentifyNewDataObjects();
				filler.IdentifyIdenticalDataObjects();
				filler.IdentifyEditedDataObjects();
				filler.IdentifyDeletedDataObjects();
				filler.UpdateFullObjectHash();
				Helpers.SendMessage("Filled temp objects table");

				// for studies with status 1, (= new) add these, their attributes,
				// their data objects, and their 
				DataTransferrer transferrer = new DataTransferrer(connstring, source);
				if (source.has_study_tables)
				{
					transferrer.AddNewStudies();
				}
				transferrer.AddNewDataObjects();


				// update studies and data objects 'data of data' for recently 
				// downloaded data that previously existed )= status of 2 and 3 
				// for both studies and data objects)

				if (source.has_study_tables)
				{
					transferrer.UpdateDateOfStudyData();
				}
				transferrer.UpdateDateOfDataObjectData();


				// then a need to examine the edited data (status = 3)
				if (source.has_study_tables)
				{
					transferrer.UpdateEditedStudyData();
				}
				transferrer.UpdateEditedDataObjectData();


				// Finally, remove any deleted stuydies / objects from the ad tables
				if (source.has_study_tables)
				{
					transferrer.RemoveDeletedStudyData();
				}
				transferrer.RemoveDeletedDataObjectData();


				TempTableDropper temp_dropper = new TempTableDropper(connstring);
				temp_dropper.DeleteTempStudiesTable();
				temp_dropper.TempDataObjectsTable();
			}
		}
	}
}
