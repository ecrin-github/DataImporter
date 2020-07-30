using Dapper;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataImporter
{
	class StudyDataAdder
	{
		string db_conn;

		public StudyDataAdder(string _db_conn)
		{
			db_conn = _db_conn;
		}


		public void TransferStudies()
		{
			string sql_string = @"INSERT INTO ad.studies (sd_sid, display_title,
            title_lang_code, brief_description, bd_contains_html, data_sharing_statement,
            dss_contains_html, study_start_year, study_start_month, study_type_id, 
            study_status_id, study_enrolment, study_gender_elig_id, min_age, 
            min_age_units_id, max_age, max_age_units_id, datetime_of_data_fetch,
            record_hash, study_full_hash)
            SELECT s.sd_sid, display_title,
            title_lang_code, brief_description, bd_contains_html, data_sharing_statement,
            dss_contains_html, study_start_year, study_start_month, study_type_id, 
            study_status_id, study_enrolment, study_gender_elig_id, min_age, 
            min_age_units_id, max_age, max_age_units_id, datetime_of_data_fetch,
            record_hash, study_full_hash 
            FROM sd.studies s
            INNER JOIN ad.temp_studies ts
            ON s.sd_sid = ts.sd_sid
            where ts.status = 1";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void TransferStudyIdentifiers()
		{
			string sql_string = @"INSERT INTO ad.study_identifiers(sd_sid,
            identifier_value, identifier_type_id, identifier_org_id, identifier_org,
            identifier_date, identifier_link, record_hash)
            SELECT s.sd_id, 
            identifier_value, identifier_type_id, identifier_org_id, identifier_org,
            identifier_date, identifier_link, record_hash
            FROM sd.study_identifiers s
            INNER JOIN ad.temp_studies ts
            ON s.sd_sid = ts.sd_sid
            where ts.status = 1";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void TransferStudyTitles()
		{
			string sql_string = @"INSERT INTO ad.study_titles(sd_sid,
            title_text, title_type_id, title_lang_code, lang_usage_id,
            is_default, comments, comparison_text, record_hash)
            SELECT s.sd_id, 
            title_text, title_type_id, title_lang_code, lang_usage_id,
            is_default, comments, comparison_text, record_hash
            FROM sd.study_titles s
            INNER JOIN ad.temp_studies ts
            ON s.sd_sid = ts.sd_sid
            where ts.status = 1"; 

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void TransferStudyReferences()
		{
			string sql_string = @"INSERT INTO ad.study_references(sd_sid,
            pmid, citation, doi, comments, record_hash)
            SELECT s.sd_id, 
            pmid, citation, doi, comments, record_hash
            FROM sd.study_references s
            INNER JOIN ad.temp_studies ts
            ON s.sd_sid = ts.sd_sid
            where ts.status = 1";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void TransferStudyContributors()
		{
			string sql_string = @"INSERT INTO ad.study_contributors(sd_sid,
            contrib_type_id, is_individual, organisation_id, organisation_name,
            person_id, person_given_name, person_family_name, person_full_name,
            person_identifier, identifier_type, person_affiliation, affil_org_id,
            affil_org_id_type, record_hash)
            SELECT s.sd_id, 
            contrib_type_id, is_individual, organisation_id, organisation_name,
            person_id, person_given_name, person_family_name, person_full_name,
            person_identifier, identifier_type, person_affiliation, affil_org_id,
            affil_org_id_type, record_hash
            FROM sd.study_contributors s
            INNER JOIN ad.temp_studies ts
            ON s.sd_sid = ts.sd_sid
            where ts.status = 1";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void TransferStudyTopics()
		{
			string sql_string = @"INSERT INTO ad.study_topics(sd_sid,
            topic_type_id, topic_value, topic_ct_id, topic_ct_code,
            where_found, record_hash)
            SELECT s.sd_id, 
            topic_type_id, topic_value, topic_ct_id, topic_ct_code,
            where_found, record_hash
            FROM sd.study_topics s
            INNER JOIN ad.temp_studies ts
            ON s.sd_sid = ts.sd_sid
            where ts.status = 1";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void TransferStudyRelationships()
		{
			string sql_string = @"INSERT INTO ad.study_relationships(sd_sid,
            relationship_type_id, target_sd_id, record_hash)
            SELECT s.sd_id, 
            relationship_type_id, target_sd_id, record_hash
            FROM sd.study_relationships s
            INNER JOIN ad.temp_studies ts
            ON s.sd_sid = ts.sd_sid
            where ts.status = 1";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void TransferStudyFeatures()
		{
			string sql_string = @"INSERT INTO ad.study_features(sd_sid,
			feature_type_id, feature_value_id, record_hash)
            SELECT s.sd_id, 
            feature_type_id, feature_value_id, record_hash
            FROM sd.study_features s
            INNER JOIN ad.temp_studies ts
            ON s.sd_sid = ts.sd_sid
            where ts.status = 1";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void TransferStudyLinks()
		{
			string sql_string = @"INSERT INTO ad.study_links(sd_sid,
			link_label, link_url, record_hash)
            SELECT s.sd_id, 
            link_label, link_url, record_hash
            FROM sd.study_links s
            INNER JOIN ad.temp_studies ts
            ON s.sd_sid = ts.sd_sid
            where ts.status = 1";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void TransferStudyIpdAvailable()
		{
			string sql_string = @"INSERT INTO ad.study_ipd_available(sd_sid,
			ipd_id, ipd_type, ipd_url, ipd_comment, record_hash)
            SELECT s.sd_id, 
            ipd_id, ipd_type, ipd_url, ipd_comment, record_hash
            FROM sd.study_ipd_available s
            INNER JOIN ad.temp_studies ts
            ON s.sd_sid = ts.sd_sid
            where ts.status = 1";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}


		public void TransferStudyHashes()
		{
			string sql_string = @"INSERT INTO ad.study_hashes(sd_sid,
			hash_type_id, composite_hash)
			SELECT s.sd_id, 
            hash_type_id, composite_hash
			FROM sd.study_hashes s
			INNER JOIN ad.temp_studies ts
            ON s.sd_sid = ts.sd_sid
            where ts.status = 1";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}
	}
}
