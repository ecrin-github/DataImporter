using Dapper;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataImporter
{
	class DataTransferrer
	{
		string conn_string;
		Source source;

		public DataTransferrer(string _conn_string, Source _source)
		{
			conn_string = _conn_string;
			source = _source;
		}


        public void AddNewStudies()
        {
			StudyDataAdder adder = new StudyDataAdder(conn_string);
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
			//if (source.has_study_links) adder.TransferStudyLinks();
			//if (source.has_study_ipd_available) adder.TransferStudyIpdAvailable();
			Helpers.SendMessage("Added new source specific study data");

		}


		public void AddNewDataObjects()
		{
			DataObjectDataAdder adder = new DataObjectDataAdder(conn_string);
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
		}


	    public void UpdateDateOfStudyData()
        {
			string sql_string = @"with t as (   
              select s.sd_sid, s.datetime_of_data_fetch 
              from sd.studies s
              inner join ad.temp_studies ts
              on s.sd_sid  = ts.sd_sid
              where ts.status in (2,3)  )
          update ad.studies 
          set datetime_of_data_fetch = t.datetime_of_data_fetch
          from t
          where sd_sid = t.sd_sid";

			using (var conn = new Npgsql.NpgsqlConnection(conn_string))
			{
				conn.Execute(sql_string);
			}

		}


		public void UpdateDateOfDataObjectData()
        {
			string sql_string = @"with t as (   
              select d.sd_oid, d.sd_sid, d.datetime_of_data_fetch 
              from sd.data_objects d
              inner join ad.temp_data_objects td
              on d.sd_oid  = td.sd_oid
              and d.sd_sid  = td.sd_sid
              where td.status in (2,3)  )
          update ad.data_objects 
          set datetime_of_data_fetch = t.datetime_of_data_fetch
          from t
          where sd_oid = t.sd_oid
          and sd_sid = t.sd_sid";

			using (var conn = new Npgsql.NpgsqlConnection(conn_string))
			{
				conn.Execute(sql_string);
			}


		}


		public void UpdateEditedStudyData()
		{
			StudyDataEditor editor = new StudyDataEditor(conn_string);
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
			//if (source.has_study_links) editor.EditStudyLinks();
			//if (source.has_study_ipd_available) editor.EditStudyIpdAvailable();
		}


		public void UpdateEditedDataObjectData()
		{
			DataObjectDataEditor editor = new DataObjectDataEditor(conn_string);
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

		}


		public void RemoveDeletedStudyData()
		{


		}


		public void RemoveDeletedDataObjectData()
		{


		}

	}

}
