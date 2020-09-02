using Dapper;
using Npgsql;

namespace DataImporter
{
	public class StudyTableBuildersAD
	{
		string connstring;

		public StudyTableBuildersAD(string _connstring)
		{
			connstring = _connstring;
		}

		public void create_ad_schema()
		{
			string sql_string = @"CREATE SCHEMA IF NOT EXISTS ad;";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}

		public void create_table_studies()
		{
			string sql_string = @"CREATE TABLE IF NOT EXISTS ad.studies(
				sd_sid                 VARCHAR         PRIMARY KEY
			  , display_title          VARCHAR         NULL
              , title_lang_code        VARCHAR         NULL default 'en'
			  , brief_description      VARCHAR         NULL
              , bd_contains_html       BOOLEAN         NULL	default false
			  , data_sharing_statement VARCHAR         NULL
              , dss_contains_html      BOOLEAN         NULL	default false
			  , study_start_year       INT             NULL
			  , study_start_month      INT             NULL
			  , study_type_id          INT             NULL
			  , study_status_id        INT             NULL
			  , study_enrolment        INT             NULL
			  , study_gender_elig_id   INT             NULL
			  , min_age                INT             NULL
			  , min_age_units_id       INT             NULL
			  , max_age                INT             NULL
			  , max_age_units_id       INT             NULL
			  , datetime_of_data_fetch TIMESTAMPTZ     NULL
              , record_hash            CHAR(32)        NULL
              , study_full_hash        CHAR(32)        NULL
              , added_on               TIMESTAMPTZ     NOT NULL default now()
              , last_edited_on         TIMESTAMPTZ     NOT NULL default now()
              , exported_on            TIMESTAMPTZ     NULL
              , record_status_id       INT             NOT NULL default 1
			);
            CREATE INDEX studies_sid ON ad.studies(sd_sid);
            CREATE INDEX studies_hash ON ad.studies(record_hash);
            CREATE INDEX studies_full_hash ON ad.studies(study_full_hash);";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_identifiers()
		{
			string sql_string = @"CREATE TABLE IF NOT EXISTS ad.study_identifiers(
                sd_sid                 VARCHAR         NOT NULL
			  , identifier_value       VARCHAR         NULL
			  , identifier_type_id     INT             NULL
			  , identifier_org_id      INT             NULL
			  , identifier_org         VARCHAR         NULL
			  , identifier_date        VARCHAR         NULL
			  , identifier_link        VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
              , added_on               TIMESTAMPTZ     NOT NULL default now()
              , last_edited_on         TIMESTAMPTZ     NOT NULL default now()
              , exported_on            TIMESTAMPTZ     NULL
              , record_status_id       INT             NOT NULL default 1
			);
            CREATE INDEX study_identifiers_sid ON ad.study_identifiers(sd_sid);";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_relationships()
		{
			string sql_string = @"CREATE TABLE IF NOT EXISTS ad.study_relationships(
                sd_sid                 VARCHAR         NOT NULL
			  , relationship_type_id   INT             NULL
			  , target_sd_sid          VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
              , added_on               TIMESTAMPTZ     NOT NULL default now()
              , last_edited_on         TIMESTAMPTZ     NOT NULL default now()
              , exported_on            TIMESTAMPTZ     NULL
              , record_status_id       INT             NOT NULL default 1
			);
            CREATE INDEX study_relationships_sid ON ad.study_relationships(sd_sid);
            CREATE INDEX study_relationships_target_sid ON ad.study_relationships(target_sd_sid);";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_references()
		{
			string sql_string = @"CREATE TABLE IF NOT EXISTS ad.study_references(
                sd_sid                 VARCHAR         NOT NULL
			  , pmid                   VARCHAR         NULL
			  , citation               VARCHAR         NULL
			  , doi                    VARCHAR         NULL	
			  , comments               VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
              , added_on               TIMESTAMPTZ     NOT NULL default now()
              , last_edited_on         TIMESTAMPTZ     NOT NULL default now()
              , exported_on            TIMESTAMPTZ     NULL
              , record_status_id       INT             NOT NULL default 1
			);
            CREATE INDEX study_references_sid ON ad.study_references(sd_sid);";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_titles()
		{
			string sql_string = @"CREATE TABLE IF NOT EXISTS ad.study_titles(
                sd_sid                 VARCHAR         NOT NULL
			  , title_text             VARCHAR         NULL
			  , title_type_id          INT             NULL
			  , title_lang_code        VARCHAR         NOT NULL default 'en'
			  , lang_usage_id          INT             NOT NULL default 11
			  , is_default             BOOLEAN         NULL
			  , comments               VARCHAR         NULL
			  , comparison_text        VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
              , added_on               TIMESTAMPTZ     NOT NULL default now()
              , last_edited_on         TIMESTAMPTZ     NOT NULL default now()
              , exported_on            TIMESTAMPTZ     NULL
              , record_status_id       INT             NOT NULL default 1
			);
            CREATE INDEX study_titles_sid ON ad.study_titles(sd_sid);";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_contributors()
		{
			string sql_string = @"CREATE TABLE IF NOT EXISTS ad.study_contributors(
				sd_sid                 VARCHAR         NOT NULL
			  , contrib_type_id        INT             NULL
			  , is_individual          BOOLEAN         NULL
			  , organisation_id        INT             NULL
              , organisation_name      VARCHAR         NULL
			  , person_id              INT             NULL
			  , person_given_name      VARCHAR         NULL
			  , person_family_name     VARCHAR         NULL
			  , person_full_name       VARCHAR         NULL
			  , person_identifier      VARCHAR         NULL
			  , identifier_type        VARCHAR         NULL
			  , person_affiliation     VARCHAR         NULL
			  , affil_org_id           VARCHAR         NULL
			  , affil_org_id_type      VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
              , added_on               TIMESTAMPTZ     NOT NULL default now()
              , last_edited_on         TIMESTAMPTZ     NOT NULL default now()
              , exported_on            TIMESTAMPTZ     NULL
              , record_status_id       INT             NOT NULL default 1
			);
            CREATE INDEX study_contributors_sid ON ad.study_contributors(sd_sid);";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_topics()
		{
			string sql_string = @"CREATE TABLE IF NOT EXISTS ad.study_topics(
				sd_sid                 VARCHAR         NOT NULL
			  , topic_type_id          INT             NULL
			  , topic_value            VARCHAR         NULL
			  , topic_ct_id            INT             NULL
			  , topic_ct_code          VARCHAR         NULL
			  , where_found            VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
              , added_on               TIMESTAMPTZ     NOT NULL default now()
              , last_edited_on         TIMESTAMPTZ     NOT NULL default now()
              , exported_on            TIMESTAMPTZ     NULL
              , record_status_id       INT             NOT NULL default 1
			);
            CREATE INDEX study_topics_sid ON ad.study_topics(sd_sid);";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}

		public void create_table_study_features()
		{
			string sql_string = @"CREATE TABLE ad.study_features(
				sd_sid                 VARCHAR         NOT NULL
			  , feature_type_id        INT             NULL
			  , feature_value_id       INT             NULL
              , record_hash            CHAR(32)        NULL
              , added_on               TIMESTAMPTZ     NOT NULL default now()
              , last_edited_on         TIMESTAMPTZ     NOT NULL default now()
              , exported_on            TIMESTAMPTZ     NULL
              , record_status_id       INT             NOT NULL default 1
			);
            CREATE INDEX study_features_sid ON ad.study_features(sd_sid);";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_links()
		{
			string sql_string = @"CREATE TABLE ad.study_links(
				sd_sid                 VARCHAR         NOT NULL
			  , link_label             VARCHAR         NULL
			  , link_url               VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
              , added_on               TIMESTAMPTZ     NOT NULL default now()
              , last_edited_on         TIMESTAMPTZ     NOT NULL default now()
              , exported_on            TIMESTAMPTZ     NULL
              , record_status_id       INT             NOT NULL default 1
			);
            CREATE INDEX study_links_sd_sid ON ad.study_links(sd_sid);";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}

		public void create_table_ipd_available()
		{
			string sql_string = @"CREATE TABLE ad.study_ipd_available(
				sd_sid                 VARCHAR         NOT NULL
			  , ipd_id                 VARCHAR         NULL
			  , ipd_type               VARCHAR         NULL
		      , ipd_url                VARCHAR         NULL
			  , ipd_comment            VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
              , added_on               TIMESTAMPTZ     NOT NULL default now()
              , last_edited_on         TIMESTAMPTZ     NOT NULL default now()
              , exported_on            TIMESTAMPTZ     NULL
              , record_status_id       INT             NOT NULL default 1
			);
            CREATE INDEX study_ipd_available_sd_sid ON ad.study_ipd_available(sd_sid);";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_study_hashes()
		{
			string sql_string = @"CREATE TABLE IF NOT EXISTS ad.study_hashes(
				sd_sid                 VARCHAR         NOT NULL
			  , hash_type_id           INT             NULL
			  , composite_hash         CHAR(32)        NULL
              , added_on               TIMESTAMPTZ     NOT NULL default now()
              , last_edited_on         TIMESTAMPTZ     NOT NULL default now()
              , exported_on            TIMESTAMPTZ     NULL
              , record_status_id       INT             NOT NULL default 1
			);
            CREATE INDEX study_hashes_sd_sid ON ad.study_hashes(sd_sid);
            CREATE INDEX study_hashes_composite_hash ON ad.study_hashes(composite_hash);";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}
	}


	public class ObjectTableBuildersAD
	{ 
		string connstring;

		public ObjectTableBuildersAD(string _connstring)
		{
			connstring = _connstring;
		}


	    public void create_table_data_objects()
		{
			string sql_string = @"CREATE TABLE IF NOT EXISTS ad.data_objects(
				sd_oid                 CHAR(24)        NULL
              , sd_sid                 VARCHAR         NOT NULL
			  , display_title          VARCHAR         NULL
			  , doi                    VARCHAR         NULL 
			  , doi_status_id          INT             NULL
			  , publication_year       INT             NULL
			  , object_class_id        INT             NULL
			  , object_type_id         INT             NULL
			  , managing_org_id        INT             NULL
			  , managing_org           VARCHAR         NULL
			  , access_type_id         INT             NULL
			  , access_details         VARCHAR         NULL
			  , access_details_url     VARCHAR         NULL
			  , url_last_checked       DATE            NULL
			  , add_study_contribs     BOOLEAN         NULL
			  , add_study_topics       BOOLEAN         NULL
			  , datetime_of_data_fetch TIMESTAMPTZ     NULL
              , record_hash            CHAR(32)        NULL
              , object_full_hash       CHAR(32)        NULL
              , added_on               TIMESTAMPTZ     NOT NULL default now()
              , last_edited_on         TIMESTAMPTZ     NOT NULL default now()
              , exported_on            TIMESTAMPTZ     NULL
              , record_status_id       INT             NOT NULL default 1
			);    
            CREATE INDEX data_objects_sd_oid ON ad.data_objects(sd_oid);
			CREATE INDEX data_objects_sd_sid ON ad.data_objects(sd_sid);
            CREATE INDEX data_objects_hash ON ad.data_objects(record_hash);
            CREATE INDEX data_objects_full_hash ON ad.data_objects(object_full_hash);";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_dataset_properties()
		{
			string sql_string = @"CREATE TABLE IF NOT EXISTS ad.dataset_properties(
                sd_oid                 CHAR(24)        NULL
			  , record_keys_type_id    INT             NULL 
			  , record_keys_details    VARCHAR         NULL    
			  , identifiers_type_id    INT             NULL  
			  , identifiers_details    VARCHAR         NULL    
			  , consents_type_id       INT             NULL  
              , consents_details       VARCHAR         NULL 
              , record_hash            CHAR(32)        NULL
              , added_on               TIMESTAMPTZ     NOT NULL default now()
              , last_edited_on         TIMESTAMPTZ     NOT NULL default now()
              , exported_on            TIMESTAMPTZ     NULL
              , record_status_id       INT             NOT NULL default 1
			);            
            CREATE INDEX dataset_properties_sd_oid ON ad.dataset_properties(sd_oid);";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_object_dates()
		{
			string sql_string = @"CREATE TABLE IF NOT EXISTS ad.object_dates(
                sd_oid                 CHAR(24)        NULL
			  , date_type_id           INT             NULL
			  , is_date_range          BOOLEAN         NULL default false
			  , date_as_string         VARCHAR         NULL
			  , start_year             INT             NULL
			  , start_month            INT             NULL
			  , start_day              INT             NULL
			  , end_year               INT             NULL
			  , end_month              INT             NULL
			  , end_day                INT             NULL
			  , details                VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
              , added_on               TIMESTAMPTZ     NOT NULL default now()
              , last_edited_on         TIMESTAMPTZ     NOT NULL default now()
              , exported_on            TIMESTAMPTZ     NULL
              , record_status_id       INT             NOT NULL default 1
			);
            CREATE INDEX object_dates_sd_oid ON ad.object_dates(sd_oid);";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_object_instances()
		{
			string sql_string = @"CREATE TABLE IF NOT EXISTS ad.object_instances(
                sd_oid                 CHAR(24)        NULL
			  , instance_type_id       INT             NOT NULL  default 1
			  , repository_org_id      INT             NULL
			  , repository_org         VARCHAR         NULL
			  , url                    VARCHAR         NULL
			  , url_accessible         BOOLEAN         NULL
			  , url_last_checked       DATE            NULL
			  , resource_type_id       INT             NULL
			  , resource_size          VARCHAR         NULL
			  , resource_size_units    VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
              , added_on               TIMESTAMPTZ     NOT NULL default now()
              , last_edited_on         TIMESTAMPTZ     NOT NULL default now()
              , exported_on            TIMESTAMPTZ     NULL
              , record_status_id       INT             NOT NULL default 1
			);
            CREATE INDEX object_instances_sd_oid ON ad.object_instances(sd_oid);";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_object_titles()
		{
			string sql_string = @"CREATE TABLE IF NOT EXISTS ad.object_titles(
                sd_oid                 CHAR(24)        NULL
			  , title_text             VARCHAR         NULL
			  , title_type_id          INT             NULL
			  , title_lang_code        VARCHAR         NOT NULL default 'en'
			  , lang_usage_id          INT             NOT NULL default 11
			  , is_default             BOOLEAN         NULL
			  , comments               VARCHAR         NULL
			  , comparison_text        VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
              , added_on               TIMESTAMPTZ     NOT NULL default now()
              , last_edited_on         TIMESTAMPTZ     NOT NULL default now()
              , exported_on            TIMESTAMPTZ     NULL
              , record_status_id       INT             NOT NULL default 1
			);
            CREATE INDEX object_titles_sd_oid ON ad.object_titles(sd_oid);";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_object_contributors()
		{
			string sql_string = @"CREATE TABLE IF NOT EXISTS ad.object_contributors(
                sd_oid                 CHAR(24)        NULL
			  , contrib_type_id        INT             NULL
			  , is_individual          BOOLEAN         NULL
			  , organisation_id        INT             NULL
              , organisation_name      VARCHAR         NULL
			  , person_id              INT             NULL
			  , person_given_name      VARCHAR         NULL
			  , person_family_name     VARCHAR         NULL
			  , person_full_name       VARCHAR         NULL
			  , person_identifier      VARCHAR         NULL
			  , identifier_type        VARCHAR         NULL
			  , person_affiliation     VARCHAR         NULL
			  , affil_org_id           VARCHAR         NULL
			  , affil_org_id_type      VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
              , added_on               TIMESTAMPTZ     NOT NULL default now()
              , last_edited_on         TIMESTAMPTZ     NOT NULL default now()
              , exported_on            TIMESTAMPTZ     NULL
              , record_status_id       INT             NOT NULL default 1
			);
            CREATE INDEX object_contributors_sd_oid ON ad.object_contributors(sd_oid);";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_object_topics()
		{
			string sql_string = @"CREATE TABLE IF NOT EXISTS ad.object_topics(
                sd_oid                 CHAR(24)        NULL
			  , topic_type_id          INT             NULL
			  , topic_value            VARCHAR         NULL
			  , topic_ct_id            INT             NULL
			  , topic_ct_code          VARCHAR         NULL
			  , where_found            VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
              , added_on               TIMESTAMPTZ     NOT NULL default now()
              , last_edited_on         TIMESTAMPTZ     NOT NULL default now()
              , exported_on            TIMESTAMPTZ     NULL
              , record_status_id       INT             NOT NULL default 1
			);
            CREATE INDEX object_topics_sd_oid ON ad.object_topics(sd_oid);";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_object_languages()
		{
			string sql_string = @"CREATE TABLE IF NOT EXISTS ad.object_languages(
                sd_oid                 CHAR(24)        NULL
              , lang_code              VARCHAR         NULL default 'en'
              , record_hash            CHAR(32)        NULL
              , added_on               TIMESTAMPTZ     NOT NULL default now()
              , last_edited_on         TIMESTAMPTZ     NOT NULL default now()
              , exported_on            TIMESTAMPTZ     NULL
              , record_status_id       INT             NOT NULL default 1
			);
            CREATE INDEX object_languages_sd_oid ON ad.object_languages(sd_oid);";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}

		public void create_table_object_corrections()
		{
			string sql_string = @"CREATE TABLE ad.object_corrections(
                sd_oid                 CHAR(24)        NULL
			  , ref_type               VARCHAR         NULL 
			  , ref_source             VARCHAR         NULL 
			  , pmid                   VARCHAR         NULL 
			  , pmid_version           VARCHAR         NULL 
			  , notes                  VARCHAR         NULL 
              , record_hash            CHAR(32)        NULL
              , added_on               TIMESTAMPTZ     NOT NULL default now()
              , last_edited_on         TIMESTAMPTZ     NOT NULL default now()
              , exported_on            TIMESTAMPTZ     NULL
              , record_status_id       INT             NOT NULL default 1
			);
            CREATE INDEX object_corrections_sd_oid ON ad.object_corrections(sd_oid);";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}

		public void create_table_object_descriptions()
		{
			string sql_string = @"CREATE TABLE ad.object_descriptions(
                sd_oid                 CHAR(24)        NULL
			  , description_type_id    INT             NULL
			  , label                  VARCHAR         NULL
			  , description_text       VARCHAR         NULL
              , lang_code              VARCHAR         NULL
              , contains_html          BOOLEAN         NULL
              , record_hash            CHAR(32)        NULL
              , added_on               TIMESTAMPTZ     NOT NULL default now()
              , last_edited_on         TIMESTAMPTZ     NOT NULL default now()
              , exported_on            TIMESTAMPTZ     NULL
              , record_status_id       INT             NOT NULL default 1
			);
            CREATE INDEX object_descriptions_sd_oid ON ad.object_descriptions(sd_oid);";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}

		public void create_table_object_identifiers()
		{
			string sql_string = @"CREATE TABLE ad.object_identifiers(
                sd_oid                 CHAR(24)        NULL
              , identifier_value       VARCHAR         NULL
			  , identifier_type_id     INT             NULL
			  , identifier_org_id      INT             NULL
			  , identifier_org         VARCHAR         NULL
			  , identifier_date        VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
              , added_on               TIMESTAMPTZ     NOT NULL default now()
              , last_edited_on         TIMESTAMPTZ     NOT NULL default now()
              , exported_on            TIMESTAMPTZ     NULL
              , record_status_id       INT             NOT NULL default 1
			);
            CREATE INDEX object_identifiers_sd_oid ON ad.object_identifiers(sd_oid);";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}

		public void create_table_object_links()
		{
			string sql_string = @"CREATE TABLE ad.object_links(
                sd_oid                 CHAR(24)        NULL
			  , bank_sequence          INT             NULL
			  , bank_name              VARCHAR         NULL
			  , accession_number       VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
              , added_on               TIMESTAMPTZ     NOT NULL default now()
              , last_edited_on         TIMESTAMPTZ     NOT NULL default now()
              , exported_on            TIMESTAMPTZ     NULL
              , record_status_id       INT             NOT NULL default 1
			);
            CREATE INDEX object_links_sd_oid ON ad.object_links(sd_oid);";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}

		public void create_table_object_public_types()
		{
			string sql_string = @"CREATE TABLE ad.object_public_types(
                sd_oid                 CHAR(24)        NULL
			  , type_name              VARCHAR         NULL
              , record_hash            CHAR(32)        NULL
              , added_on               TIMESTAMPTZ     NOT NULL default now()
              , last_edited_on         TIMESTAMPTZ     NOT NULL default now()
              , exported_on            TIMESTAMPTZ     NULL
              , record_status_id       INT             NOT NULL default 1
			);
            CREATE INDEX object_public_type_sd_oid ON ad.object_public_type(sd_oid);";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void create_table_object_hashes()
		{
			string sql_string = @"CREATE TABLE IF NOT EXISTS ad.object_hashes(
                sd_oid                 CHAR(24)        NULL
			  , hash_type_id           INT             NULL
              , composite_hash         CHAR(32)        NULL
              , added_on               TIMESTAMPTZ     NOT NULL default now()
              , last_edited_on         TIMESTAMPTZ     NOT NULL default now()
              , exported_on            TIMESTAMPTZ     NULL
              , record_status_id       INT             NOT NULL default 1
			);
            CREATE INDEX object_hashes_sd_oid ON ad.object_hashes(sd_oid);
            CREATE INDEX object_hashes_composite_hash ON ad.object_hashes(composite_hash);"; 

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}
	}
}

