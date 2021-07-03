using Serilog;

namespace DataImporter
{
    class DataTransferrer
    {
        ISource _source;
        ILogger _logger;
        string _connstring;

        ForeignTableManager FTM;
        StudyDataAdder study_adder;
        DataObjectDataAdder object_adder;
        StudyDataEditor study_editor;
        DataObjectDataEditor object_editor;

        public DataTransferrer(ISource source, ILogger logger)
        {
            _source = source;
            _connstring = _source.db_conn;
            _logger = logger;

            FTM = new ForeignTableManager(_connstring);
            study_adder = new StudyDataAdder(_connstring, _logger);
            object_adder = new DataObjectDataAdder(_connstring, _logger);
            study_editor = new StudyDataEditor(_connstring, _logger);
            object_editor = new DataObjectDataEditor(_connstring, _logger);
        }

        public void EstablishForeignMonTables(ICredentials creds)
        {
            FTM.EstablishMonForeignTables(creds.Username, creds.Password);
        }

        public void DropForeignMonTables()
        {
            FTM.DropMonForeignTables();
        }

        public void AddNewStudies(int import_id)
        {
            study_adder.TransferStudies();
            study_adder.TransferStudyIdentifiers();
            study_adder.TransferStudyTitles();

            // These are database dependent

            if (_source.has_study_references) study_adder.TransferStudyReferences();
            if (_source.has_study_contributors) study_adder.TransferStudyContributors();
            if (_source.has_study_topics) study_adder.TransferStudyTopics();
            if (_source.has_study_features) study_adder.TransferStudyFeatures();
            if (_source.has_study_relationships) study_adder.TransferStudyRelationships();
            if (_source.has_study_links) study_adder.TransferStudyLinks();
            if (_source.has_study_ipd_available) study_adder.TransferStudyIpdAvailable();
            _logger.Information("Added new source specific study data");

           // study_adder.UpdateStudiesLastImportedDate(import_id, source.id);

            study_adder.TransferStudyHashes();
            _logger.Information("Added new study hashes");
        }


        public void AddNewDataObjects(int import_id)
        {
            object_adder.TransferDataObjects();
            if (_source.has_object_datasets) object_adder.TransferDataSetProperties();
            object_adder.TransferObjectInstances();
            object_adder.TransferObjectTitles();

            // these are database dependent		

            if (_source.has_object_dates) object_adder.TransferObjectDates();
            if (_source.has_object_rights) object_adder.TransferObjectRights();
            if (_source.has_object_relationships) object_adder.TransferObjectRelationships();
            if (_source.has_object_pubmed_set)
            {
                object_adder.TransferObjectContributors();
                object_adder.TransferObjectTopics();
                object_adder.TransferObjectComments();
                object_adder.TransferObjectDescriptions();
                object_adder.TransferObjectIdentifiers();
                object_adder.TransferObjectDBLinks();
                object_adder.TransferObjectPublicationTypes();
            }
            _logger.Information("Added new source specific object data");

            object_adder.TransferObjectHashes();
            _logger.Information("Added new object hashes");
        }

        public void UpdateDatesOfData()
        {
            if (_source.has_study_tables)
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
            if (_source.has_study_references) study_editor.EditStudyReferences();
            if (_source.has_study_contributors) study_editor.EditStudyContributors();
            if (_source.has_study_topics) study_editor.EditStudyTopics();
            if (_source.has_study_features) study_editor.EditStudyFeatures();
            if (_source.has_study_relationships) study_editor.EditStudyRelationships();
            if (_source.has_study_links) study_editor.EditStudyLinks();
            if (_source.has_study_ipd_available) study_editor.EditStudyIpdAvailable();

            study_editor.UpdateStudiesLastImportedDate(import_id, _source.id);
            _logger.Information("Edited study data");

            study_editor.UpdateStudyCompositeHashes();
            study_editor.AddNewlyCreatedStudyHashTypes();
            study_editor.DropNewlyDeletedStudyHashTypes();
        }


        public void UpdateEditedDataObjectData(int import_id)
        {
            object_editor.EditDataObjects();
            if (_source.has_object_datasets) object_editor.EditDataSetProperties();
            object_editor.EditObjectInstances();
            object_editor.EditObjectTitles();

            // these are database dependent		

            if (_source.has_object_dates) object_editor.EditObjectDates();
            if (_source.has_object_rights) object_editor.EditObjectRights();
            if (_source.has_object_relationships) object_editor.EditObjectRelationships();
            if (_source.has_object_pubmed_set)
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

            _logger.Information("Edited data object data");
        }


        public void RemoveDeletedStudyData(int import_id)
        {
            study_editor.DeleteStudyRecords("studies");
            study_editor.DeleteStudyRecords("study_identifiers");
            study_editor.DeleteStudyRecords("study_titles");
            study_editor.DeleteStudyRecords("study_hashes"); ;

            // these are database dependent
            if (_source.has_study_references) study_editor.DeleteStudyRecords("study_references");
            if (_source.has_study_contributors) study_editor.DeleteStudyRecords("study_contributors");
            if (_source.has_study_topics) study_editor.DeleteStudyRecords("study_topics");
            if (_source.has_study_features) study_editor.DeleteStudyRecords("study_features"); ;
            if (_source.has_study_relationships) study_editor.DeleteStudyRecords("study_relationships");
            if (_source.has_study_links) study_editor.DeleteStudyRecords("study_links");
            if (_source.has_study_ipd_available) study_editor.DeleteStudyRecords("study_ipd_available");

            study_editor.UpdateStudiesDeletedDate(import_id, _source.id);

            _logger.Information("Deleted now missing study data");
        }


        public void RemoveDeletedDataObjectData(int import_id)
        {
            object_editor.DeleteObjectRecords("data_objects");
            object_editor.DeleteObjectRecords("object_instances");
            object_editor.DeleteObjectRecords("object_titles");
            object_editor.DeleteObjectRecords("object_hashes");

            // these are database dependent		

            if (_source.has_object_datasets) object_editor.DeleteObjectRecords("object_datasets");
            if (_source.has_object_dates) object_editor.DeleteObjectRecords("object_dates");
            if (_source.has_object_pubmed_set)
            {
                object_editor.DeleteObjectRecords("object_contributors"); ;
                object_editor.DeleteObjectRecords("object_topics");
                object_editor.DeleteObjectRecords("object_comments");
                object_editor.DeleteObjectRecords("object_descriptions");
                object_editor.DeleteObjectRecords("object_identifiers");
                object_editor.DeleteObjectRecords("object_db_links");
                object_editor.DeleteObjectRecords("object_publication_types"); ;
            }

            if (!_source.has_study_tables)
            {
                object_editor.UpdateObjectsDeletedDate(import_id, _source.id);
            }

            _logger.Information("Deleted now missing data object data");
        }


        public void UpdateFullStudyHashes()
        {
            if (_source.has_study_tables)
            {
                study_editor.UpdateFullStudyHash();
            }
            object_editor.UpdateFullObjectHash();
            _logger.Information("Full hash values updated");
        }


        public void UpdateStudiesLastImportedDate(int import_id)
        {
            study_editor.UpdateStudiesLastImportedDate(import_id, _source.id);
        }


        public void UpdateObjectsLastImportedDate(int import_id)
        {
            object_editor.UpdateObjectsLastImportedDate(import_id, _source.id);
        }
    }
}
