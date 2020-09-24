using Dapper;
using Npgsql;
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

            string sql_string = @"UPDATE ad.data_objects a
			 set 
             display_title = t.display_title, 
             version = t.version,
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
             eosc_category = t.eosc_category,
             add_study_contribs = t.add_study_contribs, 
             add_study_topics = t.add_study_topics,  
             datetime_of_data_fetch = t.datetime_of_data_fetch,  
             record_hash = t.record_hash, 
             last_edited_on = current_timestamp
             from (select * from sd.data_objects s
			   INNER JOIN ad.objects_catalogue ts
               ON s.sd_oid = ts.sd_oid
               where ts.object_rec_status = 2) t;";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


        public void UpdateObjectsLastImportedDate(int import_id, int source_id)
        {
            string sql_string = @"Update mon_sf.source_data_objects s
            set last_import_id = " + import_id.ToString() + @", 
            last_imported = current_timestamp
            from ad.objects_catalogue ts
            where s.sd_id = ts.sd_oid and
            s.source_id = " + source_id.ToString() + @"
			and ts.status = 2";

            using (var conn = new NpgsqlConnection(connstring))
            {
                conn.Execute(sql_string);
            }
        }


        public void EditDataSetProperties()
		{
            // if the record hash for the dataset properties has changed, then 
            // the data should be changed

            string sql_string = @"UPDATE ad.dataset_properties a
			 set 
             record_keys_type_id = t.record_keys_type_id, 
             record_keys_details = t.record_keys_details, 
             deident_type_id = t.deident_type_id, 
             deident_direct = t.deident_direct,
             deident_hipaa = t.deident_hipaa,
             deident_dates = t.deident_dates, 
             deident_nonarr = t.deident_nonarr, 
             deident_kanon = t.deident_kanon, 
             deident_details = t.deident_details,
			 consent_type_id = t.consent_type_id, 
             consent_noncommercial = t.consent_noncommercial, 
             consent_geog_restrict = t.consent_geog_restrict,
			 consent_research_type = t.consent_research_type, 
             consent_genetic_only = t.consent_genetic_only, 
             consent_no_methods = t.consent_no_methods, 
             consent_details = t.consent_details,  
             record_hash = t.record_hash, 
             last_edited_on = current_timestamp
    		   from (select * from sd.dataset_properties s
			   INNER JOIN ad.objects_catalogue ts
               ON s.sd_oid = ts.sd_oid
               where ts.object_rec_status = 2) t;"; 

            using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}

		public void EditObjectInstances()
		{
            string sql_string = @"with t as (
               SELECT sd_oid from 
               ad.objects_changed_atts 
               WHERE hash_type_id = 51)"; 

			string sql_stringD = sql_string + @"DELETE from ad.object_instances a
			USING t
			WHERE a.sd_oid = t.sd_oid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.object_instances(sd_oid, 
            instance_type_id, repository_org_id, repository_org,
            url, url_accessible, url_last_checked, resource_type_id,
            resource_size, resource_size_units, resource_comments, record_hash)
            SELECT d.sd_oid, 
            instance_type_id, repository_org_id, repository_org,
            url, url_accessible, url_last_checked, resource_type_id,
            resource_size, resource_size_units, resource_comments, record_hash
            FROM sd.object_instances d
            INNER JOIN t
            on d.sd_oid = t.sd_oid";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_stringD);
				conn.Execute(sql_stringI);
			}

		}

		public void EditObjectTitles()
		{
            string sql_string = @"with t as (
               SELECT sd_oid from 
               ad.objects_changed_atts 
               WHERE hash_type_id = 52)";

            string sql_stringD = sql_string + @"DELETE from ad.object_titles a
			USING t
			WHERE a.sd_oid = t.sd_oid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.object_titles(sd_oid, 
            title_type_id, title_text, lang_code,
            lang_usage_id, is_default, comments, comparison_text, record_hash)
            SELECT d.sd_oid, 
            title_type_id, title_text, lang_code,
            lang_usage_id, is_default, comments, comparison_text, record_hash
            FROM sd.object_titles d
            INNER JOIN t
            on d.sd_oid = t.sd_oid";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_stringD);
				conn.Execute(sql_stringI);
			}

		}


		public void EditObjectLanguages()
		{
            string sql_string = @"with t as (
               SELECT sd_oid from 
               ad.objects_changed_atts 
               WHERE hash_type_id = 58)";

            string sql_stringD = sql_string + @"DELETE from ad.object_languages a
			USING t
			WHERE a.sd_oid = t.sd_oid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.object_languages(sd_oid, 
            lang_code, record_hash)
            SELECT d.sd_oid, 
            lang_code, record_hash
            FROM sd.object_languages d
            INNER JOIN t
            on d.sd_oid = t.sd_oid";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_stringD);
				conn.Execute(sql_stringI);
			}

		}

		public void EditObjectDates()
		{
            string sql_string = @"with t as (
               SELECT sd_oid from 
               ad.objects_changed_atts 
               WHERE hash_type_id = 53)";

            string sql_stringD = sql_string + @"DELETE from ad.object_dates a
			USING t
			WHERE a.sd_oid = t.sd_oid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.object_dates(sd_oid, 
            date_type_id, is_date_range, date_as_string, start_year, 
            start_month, start_day, end_year, end_month, end_day, details, record_hash)
            SELECT d.sd_oid, 
            date_type_id, is_date_range, date_as_string, start_year, 
            start_month, start_day, end_year, end_month, end_day, details, record_hash
            FROM sd.object_dates d
            INNER JOIN t
            on d.sd_oid = t.sd_oid";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_stringD);
				conn.Execute(sql_stringI);
			}

		}

        public void EditObjectContributors()
		{
            string sql_string = @"with t as (
               SELECT sd_oid from 
               ad.objects_changed_atts 
               WHERE hash_type_id = 55)";

            string sql_stringD = sql_string + @"DELETE from ad.object_contributors a
			USING t
			WHERE a.sd_oid = t.sd_oid; ";

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

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_stringD);
				conn.Execute(sql_stringI);
			}

		}

		public void EditObjectTopics()
		{
            string sql_string = @"with t as (
               SELECT sd_oid from 
               ad.objects_changed_atts 
               WHERE hash_type_id = 54)";

            string sql_stringD = sql_string + @"DELETE from ad.object_topics a
			USING t
			WHERE a.sd_oid = t.sd_oid; ";

            string sql_stringI = sql_string + @"INSERT INTO ad.object_topics(sd_oid, 
            topic_type_id, mesh_coded, topic_code, topic_value, 
            topic_qualcode, topic_qualvalue, original_ct_id, original_ct_code,
            original_value, comments, record_hash)
            SELECT d.sd_oid,  
            topic_type_id, mesh_coded, topic_code, topic_value, 
            topic_qualcode, topic_qualvalue, original_ct_id, original_ct_code,
            original_value, comments, record_hash
            FROM sd.object_topics d
            INNER JOIN t
            on d.sd_oid = t.sd_oid";

            using (var conn = new NpgsqlConnection(connstring))
            {
                conn.Execute(sql_stringD);
                conn.Execute(sql_stringI);
            }

        }


        public void EditObjectComments()
		{
            string sql_string = @"with t as (
               SELECT sd_oid from 
               ad.objects_changed_atts 
               WHERE hash_type_id = 61)";

            string sql_stringD = sql_string + @"DELETE from ad.object_corrections a
			USING t
			WHERE a.sd_oid = t.sd_oid; ";

            string sql_stringI = sql_string + @"INSERT INTO ad.object_corrections(sd_oid, 
            ref_type, ref_source, pmid, pmid_version, notes, record_hash)
            SELECT d.sd_oid,  
            ref_type, ref_source, pmid, pmid_version, notes, record_hash
            FROM sd.object_corrections d
            INNER JOIN t
            on d.sd_oid = t.sd_oid";

            using (var conn = new NpgsqlConnection(connstring))
            {
                conn.Execute(sql_stringD);
                conn.Execute(sql_stringI);
            }

        }


        public void EditObjectDescriptions()
		{
            string sql_string = @"with t as (
               SELECT sd_oid from 
               ad.objects_changed_atts 
               WHERE hash_type_id = 57)";

            string sql_stringD = sql_string + @"DELETE from ad.object_descriptions a
			USING t
			WHERE a.sd_oid = t.sd_oid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.object_descriptions(sd_oid, 
            description_type_id, label, description_text, lang_code, 
            contains_html, record_hash)
            SELECT d.sd_oid, 
            description_type_id, label, description_text, lang_code, 
            contains_html, record_hash
            FROM sd.object_descriptions d
            INNER JOIN t
            on d.sd_oid = t.sd_oid";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_stringD);
				conn.Execute(sql_stringI);
			}

		}


		public void EditObjectIdentifiers()
		{
            string sql_string = @"with t as (
               SELECT sd_oid from 
               ad.objects_changed_atts 
               WHERE hash_type_id = 63)";

            string sql_stringD = sql_string + @"DELETE from ad.object_identifiers a
			USING t
			WHERE a.sd_oid = t.sd_oid; ";

            string sql_stringI = sql_string + @"INSERT INTO ad.object_identifiers(sd_oid, 
            identifier_value, identifier_type_id, identifier_org_id, identifier_org,
            identifier_date, record_hash)
            SELECT d.sd_oid, 
            identifier_value, identifier_type_id, identifier_org_id, identifier_org,
            identifier_date, record_hash
            FROM sd.object_identifiers d
            INNER JOIN t
            on d.sd_oid = t.sd_oid";

            using (var conn = new NpgsqlConnection(connstring))
            {
                conn.Execute(sql_stringD);
                conn.Execute(sql_stringI);
            }

        }


        public void EditObjectDBLinks()
		{
            string sql_string = @"with t as (
               SELECT sd_oid from 
               ad.objects_changed_atts 
               WHERE hash_type_id = 60)";

            string sql_stringD = sql_string + @"DELETE from ad.object_links a
			USING t
			WHERE a.sd_oid = t.sd_oid; ";

            string sql_stringI = sql_string + @"INSERT INTO ad.object_links(sd_oid, 
            db_sequence, db_name, id_in_db, record_hash)
            SELECT d.sd_oid, 
            db_sequence, db_name, id_in_db, record_hash
            FROM sd.object_links d
            INNER JOIN t
            on d.sd_oid = t.sd_oid";

            using (var conn = new NpgsqlConnection(connstring))
            {
                conn.Execute(sql_stringD);
                conn.Execute(sql_stringI);
            }

        }


        public void EditObjectPublicationTypes()
		{
            string sql_string = @"with t as (
               SELECT sd_oid from 
               ad.objects_changed_atts 
               WHERE hash_type_id = 62)";

            string sql_stringD = sql_string + @"DELETE from ad.object_public_types a
			USING t
			WHERE a.sd_oid = t.sd_oid; ";

            string sql_stringI = sql_string + @"INSERT INTO ad.object_public_types(sd_oid, 
            type_name, record_hash)
            SELECT d.sd_oid, 
            type_name, record_hash
            FROM sd.object_public_types d
            INNER JOIN t
            on d.sd_oid = t.sd_oid";

            using (var conn = new NpgsqlConnection(connstring))
            {
                conn.Execute(sql_stringD);
                conn.Execute(sql_stringI);
            }

        }


        public void EditObjectRelationships()
        {
            string sql_string = @"with t as (
               SELECT sd_oid from 
               ad.objects_changed_atts 
               WHERE hash_type_id = 56)";

            string sql_stringD = sql_string + @"DELETE from ad.object_public_types a
			USING t
			WHERE a.sd_oid = t.sd_oid; ";

            string sql_stringI = sql_string + @"INSERT INTO ad.object_public_types(sd_oid, 
            relationship_type_id, target_sd_oid, record_hash)
            SELECT d.sd_oid, 
            relationship_type_id, target_sd_oid, record_hash
            FROM sd.object_public_types d
            INNER JOIN t
            on d.sd_oid = t.sd_oid";

            using (var conn = new NpgsqlConnection(connstring))
            {
                conn.Execute(sql_stringD);
                conn.Execute(sql_stringI);
            }

        }


        public void EditObjectRights()
        {
            string sql_string = @"with t as (
               SELECT sd_oid from 
               ad.objects_changed_atts 
               WHERE hash_type_id = 59)";

            string sql_stringD = sql_string + @"DELETE from ad.object_public_types a
			USING t
			WHERE a.sd_oid = t.sd_oid; ";

            string sql_stringI = sql_string + @"INSERT INTO ad.object_public_types(sd_oid, 
            rights_name, rights_uri, comments, record_hash)
            SELECT d.sd_oid, 
            rights_name, rights_uri, comments, record_hash
            FROM sd.object_public_types d
            INNER JOIN t
            on d.sd_oid = t.sd_oid";

            using (var conn = new NpgsqlConnection(connstring))
            {
                conn.Execute(sql_stringD);
                conn.Execute(sql_stringI);
            }

        }

        public void EditObjectHashes()
		{

            // Need to ensure that the hashes themselves are all up to date
            // (for the next comparison)
            // Change the ones that have been changed in sd

            string sql_string = @"UPDATE ad.object_hashes ah
            set composite_hash = sh.composite_hash
            FROM sd.object_hashes sh
            WHERE ah.sd_oid = sh.sd_oid
            and ah.hash_type_id = sh.hash_type_id
            AND ah.composite_hash <> sh.composite_hash";

            using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


        public void DeleteRecords(string table_name)
        {
            string sql_string = @"with t as (
			      select sd_oid from ad.objects_catalogue
			      where status = 4)
			  delete from ad." + table_name + @" a
              using t
			  where a.sd_oid = t.sd_oid;";

            using (var conn = new NpgsqlConnection(connstring))
            {
                conn.Execute(sql_string);
            }
        }


        public void UpdateObjectsDeletedDate(int import_id, int source_id)
        {
            string sql_string = @"Update mon_sf.source_data_objects s
            set last_import_id = " + (-1 * import_id).ToString() + @", 
            last_imported = current_timestamp
            from ad.objects_catalogue ts
            where s.sd_id = ts.sd_sid and
            s.source_id = " + source_id.ToString() + @"
            and ts.status = 4;";

            using (var conn = new NpgsqlConnection(connstring))
            {
                conn.Execute(sql_string);
            }
        }

    }

}
