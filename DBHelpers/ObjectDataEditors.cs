using Dapper;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataImporter
{
	class DataObjectDataEditor
	{
		string db_conn;

		public DataObjectDataEditor(string _db_conn)
		{
			db_conn = _db_conn;
		}


		public void EditDataObjects()
		{
			string sql_string = @"INSERT INTO ad.data_objects(study_ad_id, object_hash_id, 
            study_hash_id, sd_id, do_id, display_title, doi, doi_status_id, publication_year,
            object_class_id, object_type_id, managing_org_id, managing_org, access_type_id,
            access_details, access_details_url, url_last_checked, add_study_contribs,
            add_study_topics, datetime_of_data_fetch, record_hash, object_full_hash)
            SELECT s.ad_id, nd.object_hash_id, 
            study_hash_id, nd.sd_id, nd.do_id, d.display_title, doi, doi_status_id, publication_year,
            object_class_id, object_type_id, managing_org_id, managing_org, access_type_id,
            access_details, access_details_url, url_last_checked, add_study_contribs,
            add_study_topics, d.datetime_of_data_fetch, d.record_hash, object_full_hash
            FROM sd.data_objects d
            INNER JOIN ad.temp_new_data_objects nd
            ON d.sd_id = nd.sd_id
            AND d.do_id = nd.do_id
            INNER JOIN ad.studies s
            ON d.sd_id = s.sd_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void EditDataSetProperties()
		{
			string sql_string = @"INSERT INTO ad.dataset_properties(object_ad_id, object_hash_id, 
            sd_id, do_id, record_keys_type_id, record_keys_details, identifiers_type_id,
            identifiers_details, consents_type_id, consents_details, record_hash)
            SELECT nd.object_ad_id, nd.object_hash_id, 
            nd.sd_id, nd.do_id, record_keys_type_id, record_keys_details, identifiers_type_id,
            identifiers_details, consents_type_id, consents_details, record_hash
            FROM sd.dataset_properties d
            INNER JOIN ad.temp_new_data_objects nd
            ON d.sd_id = nd.sd_id
            AND d.do_id = nd.do_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void EditObjectInstances()
		{
			string sql_string = @"INSERT INTO ad.object_instances(object_ad_id, object_hash_id, 
            sd_id, do_id, instance_type_id, repository_org_id, repository_org,
            url, url_accessible, url_last_checked, resource_type_id,
            resource_size, resource_size_units, record_hash)
            SELECT nd.object_ad_id, nd.object_hash_id, 
            nd.sd_id, nd.do_id, instance_type_id, repository_org_id, repository_org,
            url, url_accessible, url_last_checked, resource_type_id,
            resource_size, resource_size_units, record_hash
            FROM sd.object_instances d
            INNER JOIN ad.temp_new_data_objects nd
            ON d.sd_id = nd.sd_id
            AND d.do_id = nd.do_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void EditObjectTitles()
		{
			string sql_string = @"INSERT INTO ad.object_titles(object_ad_id, object_hash_id, 
            sd_id, do_id, title_text, title_type_id, title_lang_code,
            lang_usage_id, is_default, comments, comparison_text, record_hash)
            SELECT nd.object_ad_id, nd.object_hash_id, 
            nd.sd_id, nd.do_id, title_text, title_type_id, title_lang_code,
            lang_usage_id, is_default, comments, comparison_text, record_hash
            FROM sd.object_titles d
            INNER JOIN ad.temp_new_data_objects nd
            ON d.sd_id = nd.sd_id
            AND d.do_id = nd.do_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void EditObjectLanguages()
		{
			string sql_string = @"INSERT INTO ad.object_languages(object_ad_id, object_hash_id, 
            sd_id, do_id, lang_code, record_hash)
            SELECT nd.object_ad_id, nd.object_hash_id, 
            nd.sd_id, nd.do_id, lang_code, record_hash
            FROM sd.object_languages d
            INNER JOIN ad.temp_new_data_objects nd
            ON d.sd_id = nd.sd_id
            AND d.do_id = nd.do_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void EditObjectDates()
		{
			string sql_string = @"INSERT INTO ad.object_dates(object_ad_id, object_hash_id, 
            sd_id, do_id, date_type_id, is_date_range, date_as_string, start_year, 
            start_month, start_day, end_year, end_month, end_day, details, record_hash)
            SELECT nd.object_ad_id, nd.object_hash_id, 
            nd.sd_id, nd.do_id, date_type_id, is_date_range, date_as_string, start_year, 
            start_month, start_day, end_year, end_month, end_day, details, record_hash
            FROM sd.object_dates d
            INNER JOIN ad.temp_new_data_objects nd
            ON d.sd_id = nd.sd_id
            AND d.do_id = nd.do_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void EditObjectContributors()
		{
			string sql_string = @"INSERT INTO ad.";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void EditObjectTopics()
		{
			string sql_string = @"INSERT INTO ad.";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void EditObjectCorrections()
		{
			string sql_string = @"INSERT INTO ad.";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void EditObjectDescriptions()
		{
			string sql_string = @"INSERT INTO ad.";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void EditObjectIdentifiers()
		{
			string sql_string = @"INSERT INTO ad.";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void EditObjectLinks()
		{
			string sql_string = @"INSERT INTO ad.";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void EditObjectPublic_types()
		{
			string sql_string = @"INSERT INTO ad.";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}
				
		public void EditObjectHashes()
		{
			string sql_string = @"INSERT INTO ad.object_hashes(object_ad_id, object_hash_id, 
            study_hash_id, sd_id, do_id, hash_type_id, composite_hash)
            SELECT nd.object_ad_id, nd.object_hash_id, 
            study_hash_id, nd.sd_id, nd.do_id, hash_type_id, composite_hash
            FROM sd.object_hashes d
            INNER JOIN ad.temp_new_data_objects nd
            ON d.sd_id = nd.sd_id
            AND d.do_id = nd.do_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

	}

}
