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
		StudyDataAdder study_adder;
		DataObjectDataAdder object_adder;
		StudyDataEditor study_editor;
		DataObjectDataEditor object_editor;

		public DataTransferrer(string _connString, Source _source)
		{
			connString = _connString;
			source = _source;
			FTM = new ForeignTableManager(connString);
			study_adder = new StudyDataAdder(connString);
			object_adder = new DataObjectDataAdder(connString);
			study_editor = new StudyDataEditor(connString);
			object_editor = new DataObjectDataEditor(connString);
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
			study_adder.TransferStudies();
			Helpers.SendMessage("Added new studies");

			study_adder.TransferStudyIdentifiers();
			Helpers.SendMessage("Added new study identifiers");

			study_adder.TransferStudyTitles();
			Helpers.SendMessage("Added new study titles");

			study_adder.TransferStudyHashes();
			Helpers.SendMessage("Added new study hashes");

			// these are database dependent

			if (source.has_study_references) study_adder.TransferStudyReferences();
			if (source.has_study_contributors) study_adder.TransferStudyContributors();
			if (source.has_study_topics) study_adder.TransferStudyTopics();
			if (source.has_study_features) study_adder.TransferStudyFeatures();
			if (source.has_study_relationships) study_adder.TransferStudyRelationships();
			if (source.has_study_links) study_adder.TransferStudyLinks();
			if (source.has_study_ipd_available) study_adder.TransferStudyIpdAvailable();
			Helpers.SendMessage("Added new source specific study data");

			study_adder.UpdateStudiesLastImportedDate(import_id, source.id);
		}


		public void AddNewDataObjects(int import_id)
		{
			object_adder.TransferDataObjects();
			Helpers.SendMessage("Added new data objects");

			object_adder.TransferObjectInstances();
			Helpers.SendMessage("Added new object instances");

			object_adder.TransferObjectTitles();
			Helpers.SendMessage("Added new object titles");

			object_adder.TransferObjectHashes();
			Helpers.SendMessage("Added new object hashes");

			// these are database dependent		

			if (source.has_dataset_properties) object_adder.TransferDataSetProperties();
			if (source.has_object_dates) object_adder.TransferObjectDates();
			if (source.has_object_languages) object_adder.TransferObjectLanguages();

			// if not - ?? delete and create an 'artificial' object language table?

			if (source.has_object_rights) object_adder.TransferObjectRights();
			if (source.has_object_relationships) object_adder.TransferObjectRelationships();
			if (source.has_object_pubmed_set)
			{
				object_adder.TransferObjectContributors();
				object_adder.TransferObjectTopics();
				object_adder.TransferObjectComments();
				object_adder.TransferObjectDescriptions();
				object_adder.TransferObjectIdentifiers();
				object_adder.TransferObjectDBLinks();
				object_adder.TransferObjectPublicationTypes();
			}
			Helpers.SendMessage("Added new source specific object data");

			if (!source.has_study_tables)
			{
				// only update the object source data that come without 
				// associated studies (chiefly PubMed)

				object_adder.UpdateObjectsLastImportedDate(import_id, source.id);
			}
		}


	    public void UpdateDateOfStudyData()
        {
			string sql_string = @"with t as 
            (   
                select s.sd_sid, s.datetime_of_data_fetch 
                from sd.studies s
                inner join ad.studies_catalogue ts
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
			Helpers.SendMessage("Updated dates of study data");
		}


		public void UpdateDateOfDataObjectData()
        {
			string sql_string = @"with t as 
            (   
                select d.sd_oid, d.datetime_of_data_fetch 
                from sd.data_objects d
                inner join ad.objects_catalogue td
                on d.sd_oid  = td.sd_oid
                where td.status in (2,3)  
            )
            update ad.data_objects s
            set datetime_of_data_fetch = t.datetime_of_data_fetch
            from t
            where s.sd_oid = t.sd_oid";

			using (var conn = new Npgsql.NpgsqlConnection(connString))
			{
				conn.Execute(sql_string);
			}
			Helpers.SendMessage("Updated dates of data object data");
		}


		public void UpdateEditedStudyData(int import_id)
		{
			study_editor.EditStudies();
			study_editor.EditStudyIdentifiers();
			study_editor.EditStudyTitles();
			study_editor.EditStudyHashes();

			// these are database dependent
			if (source.has_study_references) study_editor.EditStudyReferences();
			if (source.has_study_contributors) study_editor.EditStudyContributors();
			if (source.has_study_topics) study_editor.EditStudyTopics();
			if (source.has_study_features) study_editor.EditStudyFeatures();
			if (source.has_study_relationships) study_editor.EditStudyRelationships();
			if (source.has_study_links) study_editor.EditStudyLinks();
			if (source.has_study_ipd_available) study_editor.EditStudyIpdAvailable();

			study_editor.UpdateStudiesLastImportedDate(import_id, source.id);
			Helpers.SendMessage("Edited study data");
		}


		public void UpdateEditedDataObjectData(int import_id)
		{
			object_editor.EditDataObjects();
			object_editor.EditObjectInstances();
			object_editor.EditObjectTitles();
			object_editor.EditObjectHashes();

			// these are database dependent		

			if (source.has_dataset_properties) object_editor.EditDataSetProperties();
			if (source.has_object_dates) object_editor.EditObjectDates();
			if (source.has_object_languages) object_editor.EditObjectLanguages();
			if (source.has_object_rights) object_editor.EditObjectRights();
			if (source.has_object_relationships) object_editor.EditObjectRelationships();
			if (source.has_object_pubmed_set)
			{
				object_editor.EditObjectContributors();
				object_editor.EditObjectTopics();
				object_editor.EditObjectComments();
				object_editor.EditObjectDescriptions();
				object_editor.EditObjectIdentifiers();
				object_editor.EditObjectDBLinks();
				object_editor.EditObjectPublicationTypes();
			}

			if (!source.has_study_tables)
			{
				// only update the object source data that come without 
				// associated studies (chiefly PubMed)

				object_editor.UpdateObjectsLastImportedDate(import_id, source.id);
			}
			Helpers.SendMessage("Edited data object data");
		}


		public void RemoveDeletedStudyData(int import_id)
		{
			study_editor.DeleteRecords("studies");
			study_editor.DeleteRecords("study_identifiers");
			study_editor.DeleteRecords("study_titles");
			study_editor.DeleteRecords("study_hashes"); ;

			// these are database dependent
			if (source.has_study_references) study_editor.DeleteRecords("study_references");
			if (source.has_study_contributors) study_editor.DeleteRecords("study_contributors");
			if (source.has_study_topics) study_editor.DeleteRecords("study_topics");
			if (source.has_study_features) study_editor.DeleteRecords("study_features"); ;
			if (source.has_study_relationships) study_editor.DeleteRecords("study_relationships");
			if (source.has_study_links) study_editor.DeleteRecords("study_links");
			if (source.has_study_ipd_available) study_editor.DeleteRecords("study_ipd_available");

			study_editor.UpdateStudiesDeletedDate(import_id, source.id);

			Helpers.SendMessage("Deleted now missing study data");
		}


		public void RemoveDeletedDataObjectData(int import_id)
		{
			object_editor.DeleteRecords("data_objects");
			object_editor.DeleteRecords("object_instances");
			object_editor.DeleteRecords("object_titles");
			object_editor.DeleteRecords("object_hashes");

			// these are database dependent		

			if (source.has_dataset_properties) object_editor.DeleteRecords("dataset_properties"); 
			if (source.has_object_dates) object_editor.DeleteRecords("object_dates");
			if (source.has_object_languages) object_editor.DeleteRecords("object_languages");
			if (source.has_object_pubmed_set)
			{
				object_editor.DeleteRecords("object_contributors");;
				object_editor.DeleteRecords("object_topics");
				object_editor.DeleteRecords("object_comments");
				object_editor.DeleteRecords("object_descriptions");
				object_editor.DeleteRecords("object_identifiers");
				object_editor.DeleteRecords("object_db_links");
				object_editor.DeleteRecords("object_publication_types"); ;
			}

			if (!source.has_study_tables)
			{
				object_editor.UpdateObjectsDeletedDate(import_id, source.id);
			}

			Helpers.SendMessage("Deleted now missing data object data");
		}


		public void UpdateFullStudyHash()
		{
			// Ensure study_full_hash is updated to reflect new value
			// The study record itself may not have changed, so the study
			// record update above cannot be used to make the edit 

			string sql_string = @"UPDATE ad.studies a
			  set study_full_hash = s.study_full_hash
              FROM sd.studies s
			  WHERE s.sd_sid = a.sd_sid;";

			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Execute(sql_string);
			}
		}


		public void UpdateFullObjectHash()
		{
			// Ensure object_full_hash is updated to reflect new value
			// The object record itself may not have changed, so the object
			// record update above cannot be used to make the edit 

			string sql_string = @"UPDATE ad.data_objects a
			  set object_full_hash = s.object_full_hash
              FROM sd.data_objects s
			  WHERE s.sd_oid = a.sd_oid;";

			using (var conn = new NpgsqlConnection(connString))
			{
				conn.Execute(sql_string);
			}
		}

	}
}
