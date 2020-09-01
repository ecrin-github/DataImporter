using Dapper;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataImporter
{
	class DataObjectDataEditor
	{
		string connstring;

		public DataObjectDataEditor(string _connstring)
		{
            connstring = _connstring;
		}


		public void EditDataObjects()
		{
            // if the record hash for the data object has changed, then 
            // the data in the data objects records should be changed

            string sql_string = @"with t as (
			  select d.*
			  from sd.data_objects d
			  inner join ad.data_objects a
              on d.sd_oid = a.sd_oid
			  where d.record_hash <> a.record_hash
              )
             UPDATE ad.data_objects 
			 set 
             display_title = t.display_title, 
             doi = t.doi,  
             doi_status_id = t.doi_status_id,  
             publication_year = t.publication_year, 
             object_class_id = t.object_class_id,  
             object_type_id = t.object_type_id,  
             managing_org_id = t.managing_org_id,  
             managing_org = t.managing_org,  
             access_type_id = t.access_type_id, 
             access_details = t.access_details,  
             access_details_url = t.access_details_url,  
             url_last_checked = t.url_last_checked, 
             add_study_contribs = t.add_study_contribs, 
             add_study_topics = t.add_study_topics,  
             datetime_of_data_fetch = t.datetime_of_data_fetch,  
             record_hash = t.record_hash, 
             date_last_edited = " + DateTime.Now.ToString() + @",
			 status = 2
             from t
			 where sd_oid = t.sd_oid;";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


        public void UpdateObjectsLastImportedDate(int last_import_id, int source_idd)
        {

        }


        public void EditDataSetProperties()
		{
            // if the record hash for the dataset properties has changed, then 
            // the data should be changed

            string sql_string = @"with t as (
			  select d.*
			  from sd.dataset_properties d
			  inner join ad.dataset_properties a
              on d.sd_oid = a.sd_oid
			  where d.record_hash <> a.record_hash
              )
             UPDATE ad.data_objects 
			 set 
             record_keys_type_id = t.record_keys_type_id, 
             record_keys_details = t.record_keys_details, 
             identifiers_type_id = t.identifiers_type_id,
             identifiers_details = t.identifiers_details, 
             consents_type_id = t.consents_type_id, 
             consents_details = t.consents_details,  
             record_hash = t.record_hash, 
             date_last_edited = " + DateTime.Now.ToString() + @",
			 status = 2
             from t
			 where sd_oid = t.sd_oid;";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}

		public void EditObjectInstances()
		{
			string sql_string = @"with t as (
               SELECT sh.sd_oid from 
               sd.object_hashes sh
               INNER JOIN ad.object_hashes ah
               on sh.sd_oid = ah.sd_oid
               WHERE hash_type_id = 51
               AND sh.composite_hash <> ah.composite_hash
            )
            ";

			string sql_stringD = sql_string + @"DELETE from ad.object_instances
			USING t
			WHERE sd_oid = t.sd_oid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.object_instances(sd_oid, 
            instance_type_id, repository_org_id, repository_org,
            url, url_accessible, url_last_checked, resource_type_id,
            resource_size, resource_size_units, record_hash)
            SELECT d.sd_oid, 
            instance_type_id, repository_org_id, repository_org,
            url, url_accessible, url_last_checked, resource_type_id,
            resource_size, resource_size_units, record_hash
            FROM sd.object_instances d
            INNER JOIN t
            on d.sd_oid = t.sd_oid";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_stringD);
				conn.Execute(sql_stringI);
			}

		}

		public void EditObjectTitles()
		{
			string sql_string = @"with t as (
               SELECT sh.sd_oid from 
               sd.object_hashes sh
               INNER JOIN ad.object_hashes ah
               on sh.sd_oid = ah.sd_oid
               WHERE hash_type_id = 52
               AND sh.composite_hash <> ah.composite_hash
            )
            ";

			string sql_stringD = sql_string + @"DELETE from ad.object_titles
			USING t
			WHERE sd_oid = t.sd_oid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.object_titles(sd_oid, 
            title_text, title_type_id, title_lang_code,
            lang_usage_id, is_default, comments, comparison_text, record_hash)
            SELECT d.sd_oid, 
            title_text, title_type_id, title_lang_code,
            lang_usage_id, is_default, comments, comparison_text, record_hash
            FROM sd.object_titles d
            INNER JOIN t
            on d.sd_oid = t.sd_oid";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_stringD);
				conn.Execute(sql_stringI);
			}

		}


		public void EditObjectLanguages()
		{
			string sql_string = @"with t as (
               SELECT sh.sd_oid from 
               sd.object_hashes sh
               INNER JOIN ad.object_hashes ah
               on sh.sd_oid = ah.sd_oid
               WHERE hash_type_id = 58
               AND sh.composite_hash <> ah.composite_hash
            )
            ";

			string sql_stringD = sql_string + @"DELETE from ad.object_languages
			USING t
			WHERE sd_oid = t.sd_oid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.object_languages(sd_oid, 
            lang_code, record_hash)
            SELECT d.sd_oid, 
            lang_code, record_hash
            FROM sd.object_languages d
            INNER JOIN t
            on d.sd_oid = t.sd_oid";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_stringD);
				conn.Execute(sql_stringI);
			}

		}

		public void EditObjectDates()
		{
			string sql_string = @"with t as (
               SELECT sh.sd_oid from 
               sd.object_hashes sh
               INNER JOIN ad.object_hashes ah
               on sh.sd_oid = ah.sd_oid
               WHERE hash_type_id = 53
               AND sh.composite_hash <> ah.composite_hash
            )
            ";

			string sql_stringD = sql_string + @"DELETE from ad.object_dates
			USING t
			WHERE sd_oid = t.sd_oid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.object_dates(sd_oid, 
            date_type_id, is_date_range, date_as_string, start_year, 
            start_month, start_day, end_year, end_month, end_day, details, record_hash)
            SELECT d.sd_oid, 
            date_type_id, is_date_range, date_as_string, start_year, 
            start_month, start_day, end_year, end_m
            INNER JOIN t
            on d.sd_oid = t.sd_oid";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_stringD);
				conn.Execute(sql_stringI);
			}

		}

        public void EditObjectContributors()
		{
			string sql_string = @"with t as (
               SELECT sh.sd_oid from 
               sd.object_hashes sh
               INNER JOIN ad.object_hashes ah
               on sh.sd_oid = ah.sd_oid
               WHERE hash_type_id = 55
               AND sh.composite_hash <> ah.composite_hash
            )
            ";

			string sql_stringD = sql_string + @"DELETE from ad.object_contributors
			USING t
			WHERE sd_oid = t.sd_oid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.object_contributors(sd_oid, 
            contrib_type_id, is_individual, organisation_id, organisation_name,
            person_id, person_given_name, person_family_name, person_full_name,
            person_identifier, identifier_type, person_affiliation, affil_org_id,
            affil_org_id_type, record_hash)
            SELECT d.sd_oid, 
            contrib_type_id, is_individual, organisation_id, organisation_name,
            person_id, person_given_name, person_family_name, person_full_name,
            person_identifier, identifier_type, person_affiliation, affil_org_id,
            affil_org_id_type, record_hash
            FROM sd.object_contributors d
            INNER JOIN t
            on d.sd_oid = t.sd_oid";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_stringD);
				conn.Execute(sql_stringI);
			}

		}

		public void EditObjectTopics()
		{
            string sql_string = @"with t as (
               SELECT sh.sd_oid from 
               sd.object_hashes sh
               INNER JOIN ad.object_hashes ah
               on sh.sd_oid = ah.sd_oid
               WHERE hash_type_id = 54
               AND sh.composite_hash <> ah.composite_hash
            )
            ";

            string sql_stringD = sql_string + @"DELETE from ad.object_topics
			USING t
			WHERE sd_oid = t.sd_oid; ";

            string sql_stringI = sql_string + @"INSERT INTO ad.object_topics(sd_oid, 
            topic_type_id, topic_value, topic_ct_id, topic_ct_code,
            where_found, record_hash)
            SELECT d.sd_oid,  
            topic_type_id, topic_value, topic_ct_id, topic_ct_code,
            where_found, record_hash
            FROM sd.object_topics d
            INNER JOIN t
            on d.sd_oid = t.sd_oid";

            using (var conn = new Npgsql.NpgsqlConnection(connstring))
            {
                conn.Execute(sql_stringD);
                conn.Execute(sql_stringI);
            }

        }


        public void EditObjectCorrections()
		{
            string sql_string = @"with t as (
               SELECT sh.sd_oid from 
               sd.object_hashes sh
               INNER JOIN ad.object_hashes ah
               on sh.sd_oid = ah.sd_oid
               WHERE hash_type_id = 61
               AND sh.composite_hash <> ah.composite_hash
            )
            ";

            string sql_stringD = sql_string + @"DELETE from ad.object_corrections
			USING t
			WHERE sd_oid = t.sd_oid; ";

            string sql_stringI = sql_string + @"INSERT INTO ad.object_corrections(sd_oid, 
            ref_type, ref_source, pmid, pmid_version, notes, record_hash)
            SELECT d.sd_oid,  
            ref_type, ref_source, pmid, pmid_version, notes, record_hash
            FROM sd.object_corrections d
            INNER JOIN t
            on d.sd_oid = t.sd_oid";

            using (var conn = new Npgsql.NpgsqlConnection(connstring))
            {
                conn.Execute(sql_stringD);
                conn.Execute(sql_stringI);
            }

        }


        public void EditObjectDescriptions()
		{
			string sql_string = @"with t as (
               SELECT sh.sd_oid from 
               sd.object_hashes sh
               INNER JOIN ad.object_hashes ah
               on sh.sd_oid = ah.sd_oid
               WHERE hash_type_id = 57
               AND sh.composite_hash <> ah.composite_hash
            )
            ";

			string sql_stringD = sql_string + @"DELETE from ad.object_descriptions
			USING t
			WHERE sd_oid = t.sd_oid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.object_descriptions(sd_oid, 
            description_type_id, label, description_text, lang_code, 
            contains_html, record_hash)
            SELECT d.sd_oid, 
            description_type_id, label, description_text, lang_code, 
            contains_html, record_hash
            FROM sd.object_descriptions d
            INNER JOIN t
            on d.sd_oid = t.sd_oid";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_stringD);
				conn.Execute(sql_stringI);
			}

		}


		public void EditObjectIdentifiers()
		{
            string sql_string = @"with t as (
               SELECT sh.sd_oid from 
               sd.object_hashes sh
               INNER JOIN ad.object_hashes ah
               on sh.sd_oid = ah.sd_oid
               WHERE hash_type_id = 63
               AND sh.composite_hash <> ah.composite_hash
            )
            ";

            string sql_stringD = sql_string + @"DELETE from ad.object_identifiers
			USING t
			WHERE sd_oid = t.sd_oid; ";

            string sql_stringI = sql_string + @"INSERT INTO ad.object_identifiers(sd_oid, 
            identifier_value, identifier_type_id, identifier_org_id, identifier_org,
            identifier_date, record_hash)
            SELECT d.sd_oid, 
            identifier_value, identifier_type_id, identifier_org_id, identifier_org,
            identifier_date, record_hash
            FROM sd.object_identifiers d
            INNER JOIN t
            on d.sd_oid = t.sd_oid";

            using (var conn = new Npgsql.NpgsqlConnection(connstring))
            {
                conn.Execute(sql_stringD);
                conn.Execute(sql_stringI);
            }

        }


        public void EditObjectLinks()
		{
            string sql_string = @"with t as (
               SELECT sh.sd_oid from 
               sd.object_hashes sh
               INNER JOIN ad.object_hashes ah
               on sh.sd_oid = ah.sd_oid
               WHERE hash_type_id = 60
               AND sh.composite_hash <> ah.composite_hash
            )
            ";

            string sql_stringD = sql_string + @"DELETE from ad.object_links
			USING t
			WHERE sd_oid = t.sd_oid; ";

            string sql_stringI = sql_string + @"INSERT INTO ad.object_links(sd_oid, 
            bank_sequence, bank_name, accession_number, record_hash)
            SELECT d.sd_oid, 
            bank_sequence, bank_name, accession_number, record_hash
            FROM sd.object_links d
            INNER JOIN t
            on d.sd_oid = t.sd_oid";

            using (var conn = new Npgsql.NpgsqlConnection(connstring))
            {
                conn.Execute(sql_stringD);
                conn.Execute(sql_stringI);
            }

        }


        public void EditObjectPublic_types()
		{
            string sql_string = @"with t as (
               SELECT sh.sd_oid from 
               sd.object_hashes sh
               INNER JOIN ad.object_hashes ah
               on sh.sd_oid = ah.sd_oid
               WHERE hash_type_id = 62
               AND sh.composite_hash <> ah.composite_hash
            )
            ";

            string sql_stringD = sql_string + @"DELETE from ad.object_public_types
			USING t
			WHERE sd_oid = t.sd_oid; ";

            string sql_stringI = sql_string + @"INSERT INTO ad.object_public_types(sd_oid, 
            type_name, record_hash)
            SELECT d.sd_oid, 
            type_name, record_hash
            FROM sd.object_public_types d
            INNER JOIN t
            on d.sd_oid = t.sd_oid";

            using (var conn = new Npgsql.NpgsqlConnection(connstring))
            {
                conn.Execute(sql_stringD);
                conn.Execute(sql_stringI);
            }

        }


        public void EditObjectHashes()
		{

            // Need to ensure that the hashes themselves are all up to date
            // Change the ones that have been changed in sd

            string sql_string = @"UPDATE ad.object_hashes ah
            set composite_hash = sh.composite_hash
            FROM sd.object_hashes sh
            WHERE ah.sd_oid = sh.sd_oid
            and ah.hash_type_id = sh.hash_type_id
            AND ah.composite_hash <> sh.composite_hash";

            using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}

	}

}
