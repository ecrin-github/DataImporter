using Dapper;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataImporter
{
	class StudyDataEditor
	{
		string db_conn;

		public StudyDataEditor(string _db_conn)
		{
			db_conn = _db_conn;
		}


		public void EditStudies()
		{
			string sql_string = @"INSERT INTO ad.studies (sd_id, hash_id, display_title,
            title_lang_code, brief_description, bd_contains_html, data_sharing_statement,
            dss_contains_html, study_start_year, study_start_month, study_type_id, 
            study_status_id, study_enrolment, study_gender_elig_id, min_age, 
            min_age_units_id, max_age, max_age_units_id, datetime_of_data_fetch,
            record_hash, study_full_hash)
            SELECT s.sd_id, s.hash_id, display_title,
            title_lang_code, brief_description, bd_contains_html, data_sharing_statement,
            dss_contains_html, study_start_year, study_start_month, study_type_id, 
            study_status_id, study_enrolment, study_gender_elig_id, min_age, 
            min_age_units_id, max_age, max_age_units_id, datetime_of_data_fetch,
            record_hash, study_full_hash 
            FROM sd.studies s
            INNER JOIN ad.temp_new_studies ns
            ON s.sd_id = ns.sd_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void EditStudyIdentifiers()
		{
			string sql_string = @"INSERT INTO ad.study_identifiers(study_ad_id, study_hash_id, sd_id,
            identifier_value, identifier_type_id, identifier_org_id, identifier_org,
            identifier_date, identifier_link, record_hash)
            SELECT ns.study_ad_id, ns.study_hash_id, ns.sd_id, 
            identifier_value, identifier_type_id, identifier_org_id, identifier_org,
            identifier_date, identifier_link, record_hash
            FROM sd.study_identifiers s
            INNER JOIN ad.temp_new_studies ns
            ON s.sd_id = ns.sd_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void EditStudyTitles()
		{
			string sql_string = @"INSERT INTO ad.study_titles(study_ad_id, study_hash_id, sd_id,
            title_text, title_type_id, title_lang_code, lang_usage_id,
            is_default, comments, comparison_text, record_hash)
            SELECT ns.study_ad_id, ns.study_hash_id, ns.sd_id, 
            title_text, title_type_id, title_lang_code, lang_usage_id,
            is_default, comments, comparison_text, record_hash
            FROM sd.study_titles s
            INNER JOIN ad.temp_new_studies ns
            ON s.sd_id = ns.sd_id"; 

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void EditStudyReferences()
		{
			string sql_string = @"INSERT INTO ad.study_references(study_ad_id, study_hash_id, sd_id,
            pmid, citation, doi, comments, record_hash)
            SELECT ns.study_ad_id, ns.study_hash_id, ns.sd_id, 
            pmid, citation, doi, comments, record_hash
            FROM sd.study_references s
            INNER JOIN ad.temp_new_studies ns
            ON s.sd_id = ns.sd_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void EditStudyContributors()
		{
			string sql_string = @"INSERT INTO ad.study_contributors(study_ad_id, study_hash_id, sd_id,
            contrib_type_id, is_individual, organisation_id, organisation_name,
            person_id, person_given_name, person_family_name, person_full_name,
            person_identifier, identifier_type, person_affiliation, affil_org_id,
            affil_org_id_type, record_hash)
            SELECT ns.study_ad_id, ns.study_hash_id, ns.sd_id, 
            contrib_type_id, is_individual, organisation_id, organisation_name,
            person_id, person_given_name, person_family_name, person_full_name,
            person_identifier, identifier_type, person_affiliation, affil_org_id,
            affil_org_id_type, record_hash
            FROM sd.study_contributors s
            INNER JOIN ad.temp_new_studies ns
            ON s.sd_id = ns.sd_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void EditStudyTopics()
		{
			string sql_string = @"INSERT INTO ad.study_topics(study_ad_id, study_hash_id, sd_id,
            topic_type_id, topic_value, topic_ct_id, topic_ct_code,
            where_found, record_hash)
            SELECT ns.study_ad_id, ns.study_hash_id, ns.sd_id, 
            topic_type_id, topic_value, topic_ct_id, topic_ct_code,
            where_found, record_hash
            FROM sd.study_topics s
            INNER JOIN ad.temp_new_studies ns
            ON s.sd_id = ns.sd_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void EditStudyRelationships()
		{
			string sql_string = @"INSERT INTO ad.study_relationships(study_ad_id, study_hash_id, sd_id,
            relationship_type_id, target_sd_id, record_hash)
            SELECT ns.study_ad_id, ns.study_hash_id, ns.sd_id, 
            relationship_type_id, target_sd_id, record_hash
            FROM sd.study_relationships s
            INNER JOIN ad.temp_new_studies ns
            ON s.sd_id = ns.sd_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}

			// needs the target ad_id to be updated too...
			sql_string = @"Update ad.study_relationships r
            SET target_ad_id = a.ad_id
            FROM ad.studies a
            WHERE r.target_sd_id = a.sd_id;";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void EditStudyFeatures()
		{
			string sql_string = @"INSERT INTO ;";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void EditStudyLinks()
		{
			string sql_string = @"INSERT INTO ;";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void EditStudyIpdAvailable()
		{
			string sql_string = @"INSERT INTO ;";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void EditStudyHashes()
		{
			string sql_string = @"INSERT INTO ad.study_hashes(study_ad_id, study_hash_id, sd_id,
			hash_type_id, composite_hash)
			SELECT ns.study_ad_id, ns.study_hash_id, ns.sd_id, 
            hash_type_id, composite_hash
			FROM sd.study_hashes s
			INNER JOIN ad.temp_new_studies ns
			ON s.sd_id = ns.sd_id";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}
	}
}
