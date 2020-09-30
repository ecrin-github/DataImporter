using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataImporter
{
	class StudyDataAdder
	{
		string connstring;
        DBUtilities dbu;


        public StudyDataAdder(string _connstring)
		{
			connstring = _connstring;
            dbu = new DBUtilities(connstring);
		}


        #region Table data transfer

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
            INNER JOIN ad.import_study_recs ts
            ON s.sd_sid = ts.sd_sid
            where ts.status = 1";

            dbu.ExecuteTransferSQL(sql_string, "studies", "adding");
		}

		public void TransferStudyIdentifiers()
		{
			string sql_string = @"INSERT INTO ad.study_identifiers(sd_sid,
            identifier_value, identifier_type_id, identifier_org_id, identifier_org,
            identifier_date, identifier_link, record_hash)
            SELECT s.sd_sid, 
            identifier_value, identifier_type_id, identifier_org_id, identifier_org,
            identifier_date, identifier_link, record_hash
            FROM sd.study_identifiers s
            INNER JOIN ad.import_study_recs ts
            ON s.sd_sid = ts.sd_sid
            where ts.status = 1";

            dbu.ExecuteTransferSQL(sql_string, "study_identifiers", "adding");
        }

		public void TransferStudyTitles()
		{
			string sql_string = @"INSERT INTO ad.study_titles(sd_sid,
            title_type_id, title_text, lang_code, lang_usage_id,
            is_default, comments, comparison_text, record_hash)
            SELECT s.sd_sid, 
            title_type_id, title_text, lang_code, lang_usage_id,
            is_default, comments, comparison_text, record_hash
            FROM sd.study_titles s
            INNER JOIN ad.import_study_recs ts
            ON s.sd_sid = ts.sd_sid
            where ts.status = 1";

            dbu.ExecuteTransferSQL(sql_string, "study_titles", "adding");
        }

		public void TransferStudyReferences()
		{
			string sql_string = @"INSERT INTO ad.study_references(sd_sid,
            pmid, citation, doi, comments, record_hash)
            SELECT s.sd_sid, 
            pmid, citation, doi, comments, record_hash
            FROM sd.study_references s
            INNER JOIN ad.import_study_recs ts
            ON s.sd_sid = ts.sd_sid
            where ts.status = 1";

            dbu.ExecuteTransferSQL(sql_string, "study_references", "adding");
        }

		public void TransferStudyContributors()
		{
			string sql_string = @"INSERT INTO ad.study_contributors(sd_sid,
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
            INNER JOIN ad.import_study_recs ts
            ON s.sd_sid = ts.sd_sid
            where ts.status = 1";

            dbu.ExecuteTransferSQL(sql_string, "study_contributors", "adding");
        }

		public void TransferStudyTopics()
		{
			string sql_string = @"INSERT INTO ad.study_topics(sd_sid,
            topic_type_id, mesh_coded, topic_code, topic_value, 
            topic_qualcode, topic_qualvalue, original_ct_id, original_ct_code,
            original_value, comments, record_hash)
            SELECT s.sd_sid, 
            topic_type_id, mesh_coded, topic_code, topic_value, 
            topic_qualcode, topic_qualvalue, original_ct_id, original_ct_code,
            original_value, comments, record_hash
            FROM sd.study_topics s
            INNER JOIN ad.import_study_recs ts
            ON s.sd_sid = ts.sd_sid
            where ts.status = 1";

            dbu.ExecuteTransferSQL(sql_string, "study_topics", "adding");
        }


		public void TransferStudyRelationships()
		{
			string sql_string = @"INSERT INTO ad.study_relationships(sd_sid,
            relationship_type_id, target_sd_sid, record_hash)
            SELECT s.sd_sid, 
            relationship_type_id, target_sd_sid, record_hash
            FROM sd.study_relationships s
            INNER JOIN ad.import_study_recs ts
            ON s.sd_sid = ts.sd_sid
            where ts.status = 1";

            dbu.ExecuteTransferSQL(sql_string, "study_relationships", "adding");
        }


		public void TransferStudyFeatures()
		{
			string sql_string = @"INSERT INTO ad.study_features(sd_sid,
			feature_type_id, feature_value_id, record_hash)
            SELECT s.sd_sid, 
            feature_type_id, feature_value_id, record_hash
            FROM sd.study_features s
            INNER JOIN ad.import_study_recs ts
            ON s.sd_sid = ts.sd_sid
            where ts.status = 1";

            dbu.ExecuteTransferSQL(sql_string, "study_features", "adding");
        }


		public void TransferStudyLinks()
		{
			string sql_string = @"INSERT INTO ad.study_links(sd_sid,
			link_label, link_url, record_hash)
            SELECT s.sd_sid, 
            link_label, link_url, record_hash
            FROM sd.study_links s
            INNER JOIN ad.import_study_recs ts
            ON s.sd_sid = ts.sd_sid
            where ts.status = 1";

            dbu.ExecuteTransferSQL(sql_string, "study_links", "adding");
        }


		public void TransferStudyIpdAvailable()
		{
			string sql_string = @"INSERT INTO ad.study_ipd_available(sd_sid,
			ipd_id, ipd_type, ipd_url, ipd_comment, record_hash)
            SELECT s.sd_sid, 
            ipd_id, ipd_type, ipd_url, ipd_comment, record_hash
            FROM sd.study_ipd_available s
            INNER JOIN ad.import_study_recs ts
            ON s.sd_sid = ts.sd_sid
            where ts.status = 1";

            dbu.ExecuteTransferSQL(sql_string, "study_ipd_available", "adding");
        }

        #endregion


        public void UpdateStudiesLastImportedDate(int import_id, int source_id)
        {
            string top_string = @"Update mon_sf.source_data_studies src
                          set last_import_id = " + import_id.ToString() + @", 
                          last_imported = current_timestamp
                          from 
                             (select so.id, so.sd_sid 
                             FROM sd.studies so
                             INNER JOIN ad.import_study_recs ts
                             ON so.sd_sid = ts.sd_sid
                             ";
           string base_string = @" where s.sd_oid = src.sd_id and
                              src.source_id = " + source_id.ToString();

            dbu.UpdateLastImportedDate("studies", top_string, base_string, "adding");
        }


        public void TransferStudyHashes()
		{
			for (int n = 11; n < 21; n++)
			{
				string sql_string = @"INSERT INTO ad.study_hashes(sd_sid,
			      hash_type_id, composite_hash)
			      SELECT s.sd_sid, 
                  hash_type_id, composite_hash
			      FROM sd.study_hashes s
	              INNER JOIN ad.import_study_recs ts
                  ON s.sd_sid = ts.sd_sid
                  where ts.status = 1
                  and s.hash_type_id = " + n.ToString();

                dbu.ExecuteSQL(sql_string);
                StringHelpers.SendFeedback("Inserting study hashes - type " + n.ToString());
            }
		}

    }
}
