using System;
using System.Collections.Generic;
using System.Text;

namespace DataImporter
{
	class ADBuilder
	{
		private string connString;
		private Source source;
		StudyTableBuildersAD study_builder;
		ObjectTableBuildersAD object_builder;

		public ADBuilder(string _connString, Source _source)
		{
			connString = _connString;
			source = _source;
			study_builder = new StudyTableBuildersAD(connString);
			object_builder = new ObjectTableBuildersAD(connString);
		}

		public void DeleteADStudyTables()
		{
			// dropping routines include 'if exists'
			// therefore can attempt to drop all of them

			study_builder.drop_table("studies");
			study_builder.drop_table("study_identifiers");
			study_builder.drop_table("study_titles");
			study_builder.drop_table("study_contributors");
			study_builder.drop_table("study_topics");
			study_builder.drop_table("study_features");
			study_builder.drop_table("study_relationships");
			study_builder.drop_table("study_references");
			study_builder.drop_table("study_hashes");
			study_builder.drop_table("study_links");
			study_builder.drop_table("study_ipd_available");
		}

		public void DeleteADObjectTables()
		{
			// dropping routines include 'if exists'
			// therefore can attempt to drop all of them

			object_builder.drop_table("data_objects");
			object_builder.drop_table("dataset_properties");
			object_builder.drop_table("object_dates");
			object_builder.drop_table("object_instances");
			object_builder.drop_table("object_titles");
			object_builder.drop_table("object_languages");
			object_builder.drop_table("object_hashes");
			object_builder.drop_table("object_contributors");
			object_builder.drop_table("object_topics");
			object_builder.drop_table("object_comments");
			object_builder.drop_table("object_descriptions");
			object_builder.drop_table("object_identifiers");
			object_builder.drop_table("object_db_links");
			object_builder.drop_table("object_publication_types");
			object_builder.drop_table("object_relationships");
			object_builder.drop_table("object_rights");
		}


		public void BuildNewADStudyTables()
		{
			// these common to all databases

			study_builder.create_ad_schema();
			study_builder.create_table_studies();
			study_builder.create_table_study_identifiers();
			study_builder.create_table_study_titles();
			study_builder.create_table_study_hashes();

			// these are database dependent
			if (source.has_study_topics) study_builder.create_table_study_topics();
			if (source.has_study_features) study_builder.create_table_study_features();
			if (source.has_study_contributors) study_builder.create_table_study_contributors();
			if (source.has_study_references) study_builder.create_table_study_references();
			if (source.has_study_relationships) study_builder.create_table_study_relationships();
			if (source.has_study_links) study_builder.create_table_study_links();
			if (source.has_study_ipd_available) study_builder.create_table_ipd_available();

		}


		public void BuildNewADObjectTables()
		{
			// these common to all databases

			object_builder.create_table_data_objects();
			object_builder.create_table_object_instances();
			object_builder.create_table_object_titles();
			object_builder.create_table_object_hashes();

			// these are database dependent		

			if (source.has_dataset_properties) object_builder.create_table_dataset_properties();
			if (source.has_object_dates) object_builder.create_table_object_dates();
			if (source.has_object_languages) object_builder.create_table_object_languages();
			if (source.has_object_relationships) object_builder.create_table_object_relationships();
			if (source.has_object_rights) object_builder.create_table_object_rights();
			if (source.has_object_pubmed_set)
			{
				object_builder.create_table_object_contributors();
				object_builder.create_table_object_topics();
				object_builder.create_table_object_comments();
				object_builder.create_table_object_descriptions();
				object_builder.create_table_object_identifiers();
				object_builder.create_table_object_db_links();
				object_builder.create_table_object_publication_types();
			}
		}
	}

}
