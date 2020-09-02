using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataImporter
{
	class DataTransferrer
	{
		string connString;
		Source source;
		ForeignTableManager FTM;

		public DataTransferrer(string _connString, Source _source)
		{
			connString = _connString;
			source = _source;
			FTM = new ForeignTableManager(connString);
		}

		public void EstablishForeignMonTables(string user_name, string password)
        {
			FTM.EstablishMonForeignTables(user_name, password);
		}

		public void DropForeignMonTables()
		{
			FTM.DropMonForeignTables();
		}

		public void AddNewStudies(int import_id)
        {
			StudyDataAdder adder = new StudyDataAdder(connString);
			adder.TransferStudies();
			Helpers.SendMessage("Added new studies");

			adder.TransferStudyIdentifiers();
			Helpers.SendMessage("Added new study identifiers");

			adder.TransferStudyTitles();
			Helpers.SendMessage("Added new study titles");

			adder.TransferStudyHashes();
			Helpers.SendMessage("Added new study hashes");

			// these are database dependent

			if (source.has_study_references) adder.TransferStudyReferences();
			if (source.has_study_contributors) adder.TransferStudyContributors();
			if (source.has_study_topics) adder.TransferStudyTopics();
			if (source.has_study_features) adder.TransferStudyFeatures();
			if (source.has_study_relationships) adder.TransferStudyRelationships();
			if (source.has_study_links) adder.TransferStudyLinks();
			if (source.has_study_ipd_available) adder.TransferStudyIpdAvailable();
			Helpers.SendMessage("Added new source specific study data");

			adder.UpdateStudiesLastImportedDate(import_id, source.id);
		}


		public void AddNewDataObjects(int import_id)
		{
			DataObjectDataAdder adder = new DataObjectDataAdder(connString);
			adder.TransferDataObjects();
			Helpers.SendMessage("Added new data objects");

			adder.TransferObjectInstances();
			Helpers.SendMessage("Added new object instances");

			adder.TransferObjectTitles();
			Helpers.SendMessage("Added new object titles");

			adder.TransferObjectHashes();
			Helpers.SendMessage("Added new object hashes");

			// these are database dependent		

			if (source.has_dataset_properties) adder.TransferDataSetProperties();
			if (source.has_object_dates) adder.TransferObjectDates();
			if (source.has_object_languages) adder.TransferObjectLanguages();
			if (source.has_object_pubmed_set)
			{
				adder.TransferObjectContributors();
				adder.TransferObjectTopics();
				adder.TransferObjectCorrections();
				adder.TransferObjectDescriptions();
				adder.TransferObjectIdentifiers();
				adder.TransferObjectLinks();
				adder.TransferObjectPublic_types();
			}
			Helpers.SendMessage("Added new source specific object data");

			if (!source.has_study_tables)
			{
				// only update the object source data that come without 
				// associated studies (chiefly PubMed)

				adder.UpdateObjectsLastImportedDate(import_id, source.id);
			}
		}


	    public void UpdateDateOfStudyData()
        {
			string sql_string = @"with t as 
            (   
                select s.sd_sid, s.datetime_of_data_fetch 
                from sd.studies s
                inner join ad.temp_studies ts
                on s.sd_sid  = ts.sd_sid
                where ts.status in (2,3)  
            )
            update ad.studies s
            set datetime_of_data_fetch = t.datetime_of_data_fetch
            from t
            where s.sd_sid = t.sd_sid";

			using (var conn = new Npgsql.NpgsqlConnection(connString))
			{
				conn.Execute(sql_string);
			}
		}


		public void UpdateDateOfDataObjectData()
        {
			string sql_string = @"with t as 
            (   
                select d.sd_oid, d.sd_sid, d.datetime_of_data_fetch 
                from sd.data_objects d
                inner join ad.temp_data_objects td
                on d.sd_oid  = td.sd_oid
                and d.sd_sid  = td.sd_sid
                where td.status in (2,3)  
            )
            update ad.data_objects s
            set datetime_of_data_fetch = t.datetime_of_data_fetch
            from t
            where s.sd_oid = t.sd_oid
            and s.sd_sid = t.sd_sid";

			using (var conn = new Npgsql.NpgsqlConnection(connString))
			{
				conn.Execute(sql_string);
			}
		}


		public void UpdateEditedStudyData(int import_id)
		{
			StudyDataEditor editor = new StudyDataEditor(connString);
			editor.EditStudies();
			editor.EditStudyIdentifiers();
			editor.EditStudyTitles();
			editor.EditStudyHashes();

			// these are database dependent
			if (source.has_study_references) editor.EditStudyReferences();
			if (source.has_study_contributors) editor.EditStudyContributors();
			if (source.has_study_topics) editor.EditStudyTopics();
			if (source.has_study_features) editor.EditStudyFeatures();
			if (source.has_study_relationships) editor.EditStudyRelationships();
			if (source.has_study_links) editor.EditStudyLinks();
			if (source.has_study_ipd_available) editor.EditStudyIpdAvailable();

			editor.UpdateStudiesLastImportedDate(import_id, source.id);
		}


		public void UpdateEditedDataObjectData(int import_id)
		{
			DataObjectDataEditor editor = new DataObjectDataEditor(connString);
			editor.EditDataObjects();
			editor.EditObjectInstances();
			editor.EditObjectTitles();
			editor.EditObjectHashes();

			// these are database dependent		

			if (source.has_dataset_properties) editor.EditDataSetProperties();
			if (source.has_object_dates) editor.EditObjectDates();
			if (source.has_object_languages) editor.EditObjectLanguages();
			if (source.has_object_pubmed_set)
			{
				editor.EditObjectContributors();
				editor.EditObjectTopics();
				editor.EditObjectCorrections();
				editor.EditObjectDescriptions();
				editor.EditObjectIdentifiers();
				editor.EditObjectLinks();
				editor.EditObjectPublic_types();
			}

			if (!source.has_study_tables)
			{
				// only update the object source data that come without 
				// associated studies (chiefly PubMed)

				editor.UpdateObjectsLastImportedDate(import_id, source.id);
			}
		}


		public void RemoveDeletedStudyData(int import_id)
		{
			StudyDataDeleter deleter = new StudyDataDeleter(connString);
			deleter.DeleteStudies();
			deleter.DeleteStudyIdentifiers();
			deleter.DeleteStudyTitles();
			deleter.DeleteStudyHashes();

			// these are database dependent
			if (source.has_study_references) deleter.DeleteStudyReferences();
			if (source.has_study_contributors) deleter.DeleteStudyContributors();
			if (source.has_study_topics) deleter.DeleteStudyTopics();
			if (source.has_study_features) deleter.DeleteStudyFeatures();
			if (source.has_study_relationships) deleter.DeleteStudyRelationships();
			if (source.has_study_links) deleter.DeleteStudyLinks();
			if (source.has_study_ipd_available) deleter.DeleteStudyIpdAvailable();

			deleter.UpdateStudiesDeletedDate(import_id, source.id);
		}


		public void RemoveDeletedDataObjectData(int import_id)
		{

			DataObjectDataDeleter deleter = new DataObjectDataDeleter(connString);
			deleter.DeleteDataObjects();
			deleter.DeleteObjectInstances();
			deleter.DeleteObjectTitles();
			deleter.DeleteObjectHashes();

			// these are database dependent		

			if (source.has_dataset_properties) deleter.DeleteDataSetProperties();
			if (source.has_object_dates) deleter.DeleteObjectDates();
			if (source.has_object_languages) deleter.DeleteObjectLanguages();
			if (source.has_object_pubmed_set)
			{
				deleter.DeleteObjectContributors();
				deleter.DeleteObjectTopics();
				deleter.DeleteObjectCorrections();
				deleter.DeleteObjectDescriptions();
				deleter.DeleteObjectIdentifiers();
				deleter.DeleteObjectLinks();
				deleter.DeleteObjectPublic_types();
			}

			if (!source.has_study_tables)
			{
				deleter.UpdateObjectsDeletedDate(import_id, source.id);
			}
		}
	}

}
