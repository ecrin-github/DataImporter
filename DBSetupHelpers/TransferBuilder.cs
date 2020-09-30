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
			StringHelpers.SendFeedback("Added new studies");

			study_adder.TransferStudyIdentifiers();
			StringHelpers.SendFeedback("Added new study identifiers");

			study_adder.TransferStudyTitles();
			StringHelpers.SendFeedback("Added new study titles");

			study_adder.TransferStudyHashes();
			StringHelpers.SendFeedback("Added new study hashes");

			// these are database dependent

			if (source.has_study_references) study_adder.TransferStudyReferences();
			if (source.has_study_contributors) study_adder.TransferStudyContributors();
			if (source.has_study_topics) study_adder.TransferStudyTopics();
			if (source.has_study_features) study_adder.TransferStudyFeatures();
			if (source.has_study_relationships) study_adder.TransferStudyRelationships();
			if (source.has_study_links) study_adder.TransferStudyLinks();
			if (source.has_study_ipd_available) study_adder.TransferStudyIpdAvailable();
			StringHelpers.SendFeedback("Added new source specific study data");

			study_adder.UpdateStudiesLastImportedDate(import_id, source.id);
		}


		public void AddNewDataObjects(int import_id)
		{
			object_adder.TransferDataObjects();
			StringHelpers.SendFeedback("Added new data objects");

			object_adder.TransferObjectInstances();
			StringHelpers.SendFeedback("Added new object instances");

			object_adder.TransferObjectTitles();
			StringHelpers.SendFeedback("Added new object titles");

			object_adder.TransferObjectHashes();
			StringHelpers.SendFeedback("Added new object hashes");

			// these are database dependent		

			if (source.has_dataset_properties) object_adder.TransferDataSetProperties();
			if (source.has_object_dates) object_adder.TransferObjectDates();
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
			StringHelpers.SendFeedback("Added new source specific object data");

			if (!source.has_study_tables)
			{
				// only update the object source data that come without 
				// associated studies (chiefly PubMed)

				object_adder.UpdateObjectsLastImportedDate(import_id, source.id);
			}
		}

		public void UpdateDatesOfData()
        {
			if (source.has_study_tables)
			{
				study_editor.UpdateDateOfStudyData();
			}
			object_editor.UpdateDateOfDataObjectData();
		}


		public void UpdateEditedStudyData(int import_id)
		{
			study_editor.EditStudies();
			study_editor.EditStudyIdentifiers();
			study_editor.EditStudyTitles();

			// these are database dependent
			if (source.has_study_references) study_editor.EditStudyReferences();
			if (source.has_study_contributors) study_editor.EditStudyContributors();
			if (source.has_study_topics) study_editor.EditStudyTopics();
			if (source.has_study_features) study_editor.EditStudyFeatures();
			if (source.has_study_relationships) study_editor.EditStudyRelationships();
			if (source.has_study_links) study_editor.EditStudyLinks();
			if (source.has_study_ipd_available) study_editor.EditStudyIpdAvailable();

			study_editor.UpdateStudiesLastImportedDate(import_id, source.id);
			StringHelpers.SendFeedback("Edited study data");

			study_editor.UpdateStudyCompositeHashes();
			study_editor.AddNewlyCreatedStudyHashTypes();
			study_editor.DropNewlyDeletedStudyHashTypes();
		}


		public void UpdateEditedDataObjectData(int import_id)
		{
			object_editor.EditDataObjects();
			object_editor.EditObjectInstances();
			object_editor.EditObjectTitles();

			// these are database dependent		

			if (source.has_dataset_properties) object_editor.EditDataSetProperties();
			if (source.has_object_dates) object_editor.EditObjectDates();
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

			object_editor.UpdateObjectCompositeHashes();
			object_editor.AddNewlyCreatedObjectHashTypes();
			object_editor.DropNewlyDeletedObjectHashTypes();

			if (!source.has_study_tables)
			{
				// only update the object source data that come without 
				// associated studies (chiefly PubMed)

				object_editor.UpdateObjectsLastImportedDate(import_id, source.id);
			}
			StringHelpers.SendFeedback("Edited data object data");
		}


		public void RemoveDeletedStudyData(int import_id)
		{
			study_editor.DeleteStudyRecords("studies");
			study_editor.DeleteStudyRecords("study_identifiers");
			study_editor.DeleteStudyRecords("study_titles");
			study_editor.DeleteStudyRecords("study_hashes"); ;

			// these are database dependent
			if (source.has_study_references) study_editor.DeleteStudyRecords("study_references");
			if (source.has_study_contributors) study_editor.DeleteStudyRecords("study_contributors");
			if (source.has_study_topics) study_editor.DeleteStudyRecords("study_topics");
			if (source.has_study_features) study_editor.DeleteStudyRecords("study_features"); ;
			if (source.has_study_relationships) study_editor.DeleteStudyRecords("study_relationships");
			if (source.has_study_links) study_editor.DeleteStudyRecords("study_links");
			if (source.has_study_ipd_available) study_editor.DeleteStudyRecords("study_ipd_available");

			study_editor.UpdateStudiesDeletedDate(import_id, source.id);

			StringHelpers.SendFeedback("Deleted now missing study data");
		}


		public void RemoveDeletedDataObjectData(int import_id)
		{
			object_editor.DeleteObjectRecords("data_objects");
			object_editor.DeleteObjectRecords("object_instances");
			object_editor.DeleteObjectRecords("object_titles");
			object_editor.DeleteObjectRecords("object_hashes");

			// these are database dependent		

			if (source.has_dataset_properties) object_editor.DeleteObjectRecords("dataset_properties"); 
			if (source.has_object_dates) object_editor.DeleteObjectRecords("object_dates");
			if (source.has_object_pubmed_set)
			{
				object_editor.DeleteObjectRecords("object_contributors");;
				object_editor.DeleteObjectRecords("object_topics");
				object_editor.DeleteObjectRecords("object_comments");
				object_editor.DeleteObjectRecords("object_descriptions");
				object_editor.DeleteObjectRecords("object_identifiers");
				object_editor.DeleteObjectRecords("object_db_links");
				object_editor.DeleteObjectRecords("object_publication_types"); ;
			}

			if (!source.has_study_tables)
			{
				object_editor.UpdateObjectsDeletedDate(import_id, source.id);
			}

			StringHelpers.SendFeedback("Deleted now missing data object data");
		}


		public void UpdateFullStudyHashes()
		{
			if (source.has_study_tables)
			{
				study_editor.UpdateFullStudyHash();
			}
			object_editor.UpdateFullObjectHash();
		}


	}
}
