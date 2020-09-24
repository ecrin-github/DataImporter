using Dapper;
using Npgsql;
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

			string sql_string = @"update ad.studies a
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
				  study_status_id = t.study_status_id,
				  study_enrolment = t.study_enrolment, 
				  study_gender_elig_id = t.study_gender_elig_id, 
				  min_age = t.min_age, 
				  min_age_units_id = t.min_age_units_id, 
				  max_age = t.max_age,
				  max_age_units_id = t.max_age_units_id, 
				  datetime_of_data_fetch = t.datetime_of_data_fetch,
				  record_hash = t.record_hash,
				  last_edited_on = current_timestamp
			  from (select * from sd.studies s
			        INNER JOIN ad.studies_catalogue ts
                    ON s.sd_sid = ts.sd_sid
                    where ts.study_rec_status = 2) t";


			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void UpdateStudiesLastImportedDate(int import_id, int source_id)
		{
			// redo with reference to 'temp' study tables

			string sql_string = @"Update mon_sf.source_data_studies s
            set last_import_id = " + import_id.ToString() + @", 
            last_imported = current_timestamp
            from ad.studies_catalogue ts
            where s.sd_id = ts.sd_sid and
            s.source_id = " + source_id.ToString() + @"
			and ts.status = 2";


			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void EditStudyIdentifiers()
		{
			// Where the composite hash value indicates that a change has taken place in one or more 
			// of the records replace the whole set for the relevant studies
			// Composite hash id for study identifiers = 11

			// redo here ....

			string sql_string = @"with t as (
               SELECT sd_sid from 
               ad.studies_changed_atts 
               WHERE hash_type_id = 11)";

			string sql_stringD = sql_string + @"DELETE from ad.study_identifiers a
			USING t
			WHERE a.sd_sid = t.sd_sid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.study_identifiers(sd_sid,
            identifier_value, identifier_type_id, identifier_org_id, identifier_org,
            identifier_date, identifier_link, record_hash)
            SELECT s.sd_sid, 
            identifier_value, identifier_type_id, identifier_org_id, identifier_org,
            identifier_date, identifier_link, record_hash
            FROM sd.study_identifiers s
            INNER JOIN t
            on s.sd_sid = t.sd_sid";

			using (var conn = new NpgsqlConnection(connstring))
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
               SELECT sd_sid from 
               ad.studies_changed_atts 
               WHERE hash_type_id = 12)";

			string sql_stringD = sql_string + @"DELETE from ad.study_titles a
			USING t
			WHERE a.sd_sid = t.sd_sid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.study_titles(sd_sid,
            title_text, title_type_id, lang_code, lang_usage_id,
            is_default, comments, comparison_text, record_hash)
            SELECT s.sd_sid, 
            title_text, title_type_id, lang_code, lang_usage_id,
            is_default, comments, comparison_text, record_hash
            FROM sd.study_titles s
            INNER JOIN t
            on s.sd_sid = t.sd_sid";

			using (var conn = new NpgsqlConnection(connstring))
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
               SELECT sd_sid from 
               ad.studies_changed_atts 
               WHERE hash_type_id = 17)";

			string sql_stringD = sql_string + @"DELETE from ad.study_references a
			USING t
			WHERE a.sd_sid = t.sd_sid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.study_references(sd_sid,
            pmid, citation, doi, comments, record_hash)
            SELECT s.sd_sid, 
            pmid, citation, doi, comments, record_hash
            FROM sd.study_references s
            INNER JOIN t
            on s.sd_sid = t.sd_sid;";

			using (var conn = new NpgsqlConnection(connstring))
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
               SELECT sd_sid from 
               ad.studies_changed_atts 
               WHERE hash_type_id = 15)";

			string sql_stringD = sql_string + @"DELETE from ad.study_contributors a
			USING t
			WHERE a.sd_sid = t.sd_sid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.study_contributors(sd_sid,
            contrib_type_id, is_individual, organisation_id, organisation_name,
            person_id, person_given_name, person_family_name, person_full_name,
            person_identifier, identifier_type, person_affiliation, affil_org_id,
            affil_org_id_type, record_hash)
            SELECT s.sd_sid, 
            contrib_type_id, is_individual, organisation_id, organisation_name,
            person_id, person_given_name, person_family_name, person_full_name,
            person_identifier, identifier_type, person_affiliation, affil_org_id,
            affil_org_id_type, record_hash
            FROM sd.study_contributors s
            INNER JOIN t
            on s.sd_sid = t.sd_sid";

			using (var conn = new NpgsqlConnection(connstring))
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
               SELECT sd_sid from 
               ad.studies_changed_atts 
               WHERE hash_type_id = 14)";

			string sql_stringD = sql_string + @"DELETE from ad.study_topics a
			USING t
			WHERE a.sd_sid = t.sd_sid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.study_topics(sd_sid,
            topic_type_id, mesh_coded, topic_code, topic_value, 
            topic_qualcode, topic_qualvalue, original_ct_id, original_ct_code,
            original_value, comments, record_hash)
            SELECT s.sd_sid, 
            topic_type_id, mesh_coded, topic_code, topic_value, 
            topic_qualcode, topic_qualvalue, original_ct_id, original_ct_code,
            original_value, comments, record_hash
            FROM sd.study_topics s
            INNER JOIN t
            on s.sd_sid = t.sd_sid";

			using (var conn = new NpgsqlConnection(connstring))
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
               SELECT sd_sid from 
               ad.studies_changed_atts 
               WHERE hash_type_id = 16)";

			string sql_stringD = sql_string + @"DELETE from ad.study_relationships a
			USING t
			WHERE a.sd_sid = t.sd_sid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.study_relationships(sd_sid,
            relationship_type_id, target_sd_sid, record_hash)
            SELECT s.sd_sid, 
            relationship_type_id, target_sd_sid, record_hash
            FROM sd.study_relationships s
            INNER JOIN t
            on s.sd_sid = t.sd_sid";

			using (var conn = new NpgsqlConnection(connstring))
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
               SELECT sd_sid from 
               ad.studies_changed_atts 
               WHERE hash_type_id = 13)";

			string sql_stringD = sql_string + @"DELETE from ad.study_features a
			USING t
			WHERE a.sd_sid = t.sd_sid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.study_features(sd_sid,
            feature_type_id, feature_value_id, record_hash)
            SELECT s.sd_sid, 
            feature_type_id, feature_value_id, record_hash
            FROM sd.study_features s
            INNER JOIN t
            on s.sd_sid = t.sd_sid";

			using (var conn = new NpgsqlConnection(connstring))
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
               SELECT sd_sid from 
               ad.studies_changed_atts 
               WHERE hash_type_id = 18)";

			string sql_stringD = sql_string + @"DELETE from ad.study_links a
			USING t
			WHERE a.sd_sid = t.sd_sid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.study_links(sd_sid,
            link_label, link_url, record_hash)
            SELECT s.sd_sid, 
            link_label, link_url, record_hash
            FROM sd.study_links s
            INNER JOIN t
            on s.sd_sid = t.sd_sid";

			using (var conn = new NpgsqlConnection(connstring))
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
               SELECT sd_sid from 
               ad.studies_changed_atts 
               WHERE hash_type_id = 19)";

			string sql_stringD = sql_string + @"DELETE from ad.study_ipd_available a
			USING t
			WHERE a.sd_sid = t.sd_sid; ";

			string sql_stringI = sql_string + @"INSERT INTO ad.study_ipd_available(sd_sid,
            ipd_id, ipd_type, ipd_url, ipd_comment, record_hash)
            SELECT s.sd_sid, 
            ipd_id, ipd_type, ipd_url, ipd_comment, record_hash
            FROM sd.study_ipd_available s
            INNER JOIN t
            on s.sd_sid = t.sd_sid";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_stringD);
				conn.Execute(sql_stringI);
			}
		}


		public void EditStudyHashes()
		{

			// Need to ensure that the hashes themselves are all up to date
			// (for the next comparison)
			// Change the ones that have been changed in sd
			
			string sql_string = @"Update ad.study_hashes ah
            set composite_hash = sh.composite_hash
            FROM sd.study_hashes sh
            WHERE ah.sd_sid = sh.sd_sid
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
			      select sd_sid from ad.studies_catalogue
			      where status = 4)
			  delete from ad." + table_name + @" a
              using t
			  where a.sd_sid = t.sd_sid;";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void UpdateStudiesDeletedDate(int import_id, int source_id)
		{
			string sql_string = @"Update mon_sf.source_data_studies s
            set last_import_id = " + (-1 * import_id).ToString() + @", 
            last_imported = current_timestamp
            from ad.studies_catalogue ts
            where s.sd_id = ts.sd_sid and
            s.source_id = " + source_id.ToString() + @"
			and ts.status = 4";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}

		}

	}
}
