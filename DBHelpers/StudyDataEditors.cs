using Dapper;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataImporter
{
	class StudyDataEditor
	{
		string connstring;

		public StudyDataEditor(string _connstring)
		{
			connstring = _connstring;
		}


		public void EditStudies()
		{
			// if the record hash for the study has changed, then the data in the studies records should be changed

			string sql_string = @"with t as (
			  select s.*
			  from sd.studies s
			  inner join ad.studies a
              on s.sd_sid = a.sd_sid
			  where s.record_hash <> a.record_hash
              )
			  update ad.studies
			  set
				  display_title = t.display_title,
				  title_lang_code = t.title_lang_code, 
				  brief_description = t.brief_description, 
				  bd_contains_html = t.bd_contains_html, 
				  data_sharing_statement = t.data_sharing_statement,
				  dss_contains_html = t.dss_contains_html, 
				  study_start_year = t.study_start_year,
				  study_start_month = t.study_start_month,
				  study_type_id = t.study_type_id, 
				  study_status_id = t.study_status_id
				  study_enrolment = t.study_enrolment, 
				  study_gender_elig_id = t.study_gender_elig_id, 
				  min_age = t.min_age, 
				  min_age_units_id = t.min_age_units_id, 
				  max_age = t.max_age
				  max_age_units_id = t.max_age_units_id, 
				  datetime_of_data_fetch = t.datetime_of_data_fetch,
				  record_hash = t.record_hash,
				  date_last_edited = " + DateTime.Now.ToString() + @",
				  status = 2
			  from t
			  where sd_sid = t.sd_sid;";
            

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void EditStudyIdentifiers()
		{
			// Where the composite hash value indicates that a change has taken place in one or more 
			// of the records replace the whole set for the relevant studies
			// Composite hash id for study identifiers = 11

			string sql_string = @"with t as (
               SELECT sh.sd_sid from 
               sd.study_hashes sh
               INNER JOIN ad.study_hashes ah
               on sh.sd_sid = ah.sd_sid
               WHERE hash_type_id = 11
               AND sh.composite_hash <> ah.composite_hash
            )
            ";

			string sql_stringD = sql_string + @"DELETE from ad.study_identifiers
			USING t
			WHERE sd_sid = t.sd_sid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.study_identifiers(sd_sid,
            identifier_value, identifier_type_id, identifier_org_id, identifier_org,
            identifier_date, identifier_link, record_hash)
            SELECT s.sd_id, 
            identifier_value, identifier_type_id, identifier_org_id, identifier_org,
            identifier_date, identifier_link, record_hash
            FROM sd.study_identifiers s
            INNER JOIN t
            on s.sd_sid = t.sd_sid";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_stringD);
				conn.Execute(sql_stringI);
			}

			
		}


		public void EditStudyTitles()
		{

			// Where the composite hash value indicates that a change has taken place in one or more 
			// of the records replace the whole set for the relevant studies
			// Composite hash id for study titles = 12

			string sql_string = @"with t as (
               SELECT sh.sd_sid from 
               sd.study_hashes sh
               INNER JOIN ad.study_hashes ah
               on sh.sd_sid = ah.sd_sid
               WHERE hash_type_id = 12
               AND sh.composite_hash <> ah.composite_hash
            )
            ";

			string sql_stringD = sql_string + @"DELETE from ad.study_titles
			USING t
			WHERE sd_sid = t.sd_sid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.study_titles(sd_sid,
            title_text, title_type_id, title_lang_code, lang_usage_id,
            is_default, comments, comparison_text, record_hash)
            SELECT s.sd_id, 
            title_text, title_type_id, title_lang_code, lang_usage_id,
            is_default, comments, comparison_text, record_hash
            FROM sd.study_titles s
            INNER JOIN t
            on s.sd_sid = t.sd_sid";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_stringD);
				conn.Execute(sql_stringI);
			}
		}


		public void EditStudyReferences()
		{
			// Where the composite hash value indicates that a change has taken place in one or more 
			// of the records replace the whole set for the relevant studies
			// Composite hash id for study references = 17

			string sql_string = @"with t as (
               SELECT sh.sd_sid from 
               sd.study_hashes sh
               INNER JOIN ad.study_hashes ah
               on sh.sd_sid = ah.sd_sid
               WHERE hash_type_id = 17
               AND sh.composite_hash <> ah.composite_hash
            )
            ";

			string sql_stringD = sql_string + @"DELETE from ad.study_references
			USING t
			WHERE sd_sid = t.sd_sid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.study_references(sd_sid,
            pmid, citation, doi, comments, record_hash)
            SELECT s.sd_id, 
            pmid, citation, doi, comments, record_hash
            FROM sd.study_references s
            INNER JOIN ad.temp_studies t
            INNER JOIN t
            on s.sd_sid = t.sd_sid";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_stringD);
				conn.Execute(sql_stringI);
			}
		}

		public void EditStudyContributors()
		{
			// Where the composite hash value indicates that a change has taken place in one or more 
			// of the records replace the whole set for the relevant studies
			// Composite hash id for study contributors = 15

			string sql_string = @"with t as (
               SELECT sh.sd_sid from 
               sd.study_hashes sh
               INNER JOIN ad.study_hashes ah
               on sh.sd_sid = ah.sd_sid
               WHERE hash_type_id = 15
               AND sh.composite_hash <> ah.composite_hash
            )
            ";

			string sql_stringD = sql_string + @"DELETE from ad.study_contributors
			USING t
			WHERE sd_sid = t.sd_sid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.study_contributors(sd_sid,
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
            INNER JOIN t
            on s.sd_sid = t.sd_sid";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_stringD);
				conn.Execute(sql_stringI);
			}
		}


		public void EditStudyTopics()
		{
			// Where the composite hash value indicates that a change has taken place in one or more 
			// of the records replace the whole set for the relevant studies
			// Composite hash id for study topics = 14

			string sql_string = @"with t as (
               SELECT sh.sd_sid from 
               sd.study_hashes sh
               INNER JOIN ad.study_hashes ah
               on sh.sd_sid = ah.sd_sid
               WHERE hash_type_id = 14
               AND sh.composite_hash <> ah.composite_hash
            )
            ";

			string sql_stringD = sql_string + @"DELETE from ad.study_topics
			USING t
			WHERE sd_sid = t.sd_sid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.study_topics(sd_sid,
            topic_type_id, topic_value, topic_ct_id, topic_ct_code,
            where_found, record_hash)
            SELECT s.sd_id, 
            topic_type_id, topic_value, topic_ct_id, topic_ct_code,
            where_found, record_hash
            FROM sd.study_topics s
            INNER JOIN t
            on s.sd_sid = t.sd_sid";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_stringD);
				conn.Execute(sql_stringI);
			}
		}


		public void EditStudyRelationships()
		{
			// Where the composite hash value indicates that a change has taken place in one or more 
			// of the records replace the whole set for the relevant studies
			// Composite hash id for study relationships = 16

			string sql_string = @"with t as (
               SELECT sh.sd_sid from 
               sd.study_hashes sh
               INNER JOIN ad.study_hashes ah
               on sh.sd_sid = ah.sd_sid
               WHERE hash_type_id = 16
               AND sh.composite_hash <> ah.composite_hash
            )
            ";

			string sql_stringD = sql_string + @"DELETE from ad.study_relationships
			USING t
			WHERE sd_sid = t.sd_sid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.study_relationships(sd_sid,
            relationship_type_id, target_sd_id, record_hash)
            SELECT s.sd_id, 
            relationship_type_id, target_sd_id, record_hash
            FROM sd.study_relationships s
            INNER JOIN t
            on s.sd_sid = t.sd_sid";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_stringD);
				conn.Execute(sql_stringI);
			}
		}


		public void EditStudyFeatures()
		{
			// Where the composite hash value indicates that a change has taken place in one or more 
			// of the records replace the whole set for the relevant studies
			// Composite hash id for study features = 13

			string sql_string = @"with t as (
               SELECT sh.sd_sid from 
               sd.study_hashes sh
               INNER JOIN ad.study_hashes ah
               on sh.sd_sid = ah.sd_sid
               WHERE hash_type_id = 13
               AND sh.composite_hash <> ah.composite_hash
            )
            ";

			string sql_stringD = sql_string + @"DELETE from ad.study_features
			USING t
			WHERE sd_sid = t.sd_sid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.study_features(sd_sid,
            feature_type_id, feature_value_id, record_hash)
            SELECT s.sd_id, 
            feature_type_id, feature_value_id, record_hash
            FROM sd.study_features s
            INNER JOIN t
            on s.sd_sid = t.sd_sid";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_stringD);
				conn.Execute(sql_stringI);
			}
		}


		public void EditStudyLinks()
		{
			// Where the composite hash value indicates that a change has taken place in one or more 
			// of the records replace the whole set for the relevant studies
			// Composite hash id for study links = ???

			string sql_string = @"with t as (
               SELECT sh.sd_sid from 
               sd.study_hashes sh
               INNER JOIN ad.study_hashes ah
               on sh.sd_sid = ah.sd_sid
               WHERE hash_type_id = ????
               AND sh.composite_hash <> ah.composite_hash
            )
            ";

			string sql_stringD = sql_string + @"DELETE from ad.study_links
			USING t
			WHERE sd_sid = t.sd_sid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.study_links(sd_sid,
            link_label, link_url, record_hash)
            SELECT s.sd_id, 
            link_label, link_url, record_hash
            FROM sd.study_links s
            INNER JOIN t
            on s.sd_sid = t.sd_sid";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_stringD);
				conn.Execute(sql_stringI);
			}
		}


		public void EditStudyIpdAvailable()
		{
			// Where the composite hash value indicates that a change has taken place in one or more 
			// of the records replace the whole set for the relevant studies
			// Composite hash id for study ipd available = ???

			string sql_string = @"with t as (
               SELECT sh.sd_sid from 
               sd.study_hashes sh
               INNER JOIN ad.study_hashes ah
               on sh.sd_sid = ah.sd_sid
               WHERE hash_type_id = ???
               AND sh.composite_hash <> ah.composite_hash
            )
            ";

			string sql_stringD = sql_string + @"DELETE from ad.study_ipd_available
			USING t
			WHERE sd_sid = t.sd_sid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.study_ipd_available(sd_sid,
            ipd_id, ipd_type, ipd_url, ipd_comment, record_hash)
            SELECT s.sd_id, 
            ipd_id, ipd_type, ipd_url, ipd_comment, record_hash
            FROM sd.study_ipd_available s
            INNER JOIN t
            on s.sd_sid = t.sd_sid";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_stringD);
				conn.Execute(sql_stringI);
			}
		}


		public void EditStudyHashes()
		{

			// Need to ensure that the hashes themselves are all up to date
			// Change the ones that have been changed in sd
			
			string sql_string = @"Update ad.study_hashes ah
            set composite_hash = sh.composite_hash
            FROM sd.study_hashes sh
            WHERE ah.sd_sid = sh.sd_sid
            and ah.hash_type_id = sh.hash_type_id
            AND ah.composite_hash <> sh.composite_hash";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}
	}
}
