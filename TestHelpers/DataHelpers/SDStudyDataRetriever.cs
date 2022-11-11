using Dapper;
using Npgsql;

namespace DataImporter
{
    class SDStudyDataRetriever
    {
        private string _source_id;
        private string _db_conn;

        public SDStudyDataRetriever(int source_id, string db_conn)
        {
            _source_id = source_id.ToString();
            _db_conn = db_conn;
        }


        private void Execute_SQL(string sql_string)
        {
            using (var conn = new NpgsqlConnection(_db_conn))
            {
                conn.Execute(sql_string);
            }
        }


        public void TransferStudies()
        {
            string sql_string = @"INSERT INTO sd.studies (sd_sid, display_title,
            title_lang_code, brief_description, data_sharing_statement,
            study_start_year, study_start_month, study_type_id, study_type,
            study_status_id, study_status, study_enrolment, study_gender_elig_id, study_gender_elig, 
            min_age, min_age_units_id, min_age_units, 
            max_age, max_age_units_id, max_age_units, datetime_of_data_fetch,
            record_hash, study_full_hash) 
            SELECT sd_sid, display_title,
            title_lang_code, brief_description, data_sharing_statement,
            study_start_year, study_start_month, study_type_id, study_type,
            study_status_id, study_status, study_enrolment, study_gender_elig_id, study_gender_elig,  
            min_age, min_age_units_id, min_age_units, 
            max_age, max_age_units_id, max_age_units, datetime_of_data_fetch,
            record_hash, study_full_hash 
            FROM sdcomp.studies
            where source_id = " + _source_id;

            Execute_SQL(sql_string);

        }


        public void TransferStudyIdentifiers()
        {
            string sql_string = @"INSERT INTO sd.study_identifiers(sd_sid,
            identifier_value, identifier_type_id, identifier_type, 
            identifier_org_id, identifier_org, identifier_org_ror_id, 
            identifier_date, identifier_link, record_hash)
            SELECT sd_sid,
            identifier_value, identifier_type_id, identifier_type, 
            identifier_org_id, identifier_org, identifier_org_ror_id, 
            identifier_date, identifier_link, record_hash
            FROM sdcomp.study_identifiers
            where source_id = " + _source_id;

            Execute_SQL(sql_string);

        }


        public void TransferStudyRelationships()
        {

            string sql_string = @"INSERT INTO sd.study_relationships(sd_sid,
            relationship_type_id, relationship_type, target_sd_sid, record_hash)
            SELECT sd_sid,
            relationship_type_id, relationship_type, target_sd_sid, record_hash
            FROM sdcomp.study_relationships
            where source_id = " + _source_id;

            Execute_SQL(sql_string);
        }


        public void TransferStudyReferences()
        {

            string sql_string = @"INSERT INTO sd.study_references(sd_sid,
            pmid, citation, doi, comments, record_hash)
            SELECT sd_sid,
            pmid, citation, doi, comments, record_hash
            FROM sdcomp.study_references
            where source_id = " + _source_id;

            Execute_SQL(sql_string);
        }


        public void TransferStudyTitles()
        {

            string sql_string = @"INSERT INTO sd.study_titles(sd_sid,
            title_type_id, title_type, title_text, lang_code, lang_usage_id,
            is_default, comments, record_hash)
            SELECT sd_sid,
            title_type_id, title_type, title_text, lang_code, lang_usage_id,
            is_default, comments, record_hash
            FROM sdcomp.study_titles
            where source_id = " + _source_id;

            Execute_SQL(sql_string);

        }


        public void TransferStudyContributors()
        {
            string sql_string = @"INSERT INTO sd.study_contributors(sd_sid, 
            contrib_type_id, is_individual, 
            person_id, person_given_name, person_family_name, person_full_name,
            orcid_id, person_affiliation, organisation_id, 
            organisation_name, organisation_ror_id, record_hash)
            SELECT sd_sid,
            contrib_type_id, is_individual, 
            person_id, person_given_name, person_family_name, person_full_name,
            orcid_id, person_affiliation, organisation_id, 
            organisation_name, organisation_ror_id, record_hash
            FROM sdcomp.study_contributors
            where source_id = " + _source_id;

            Execute_SQL(sql_string);

        }


        public void TransferStudyTopics()
        {
            string sql_string = @"INSERT INTO sd.study_topics(sd_sid,
            topic_type_id, mesh_coded, mesh_code, mesh_value, 
            original_ct_id, original_ct_code,
            original_value, record_hash)
            SELECT sd_sid,
            topic_type_id, mesh_coded, mesh_code, mesh_value, 
            original_ct_id, original_ct_code,
            original_value, record_hash
            FROM sdcomp.study_topics
            where source_id = " + _source_id;

            Execute_SQL(sql_string);
        }


        public void TransferStudyFeatures()
        {
            string sql_string = @"INSERT INTO sd.study_features(sd_sid,
            feature_type_id, feature_type, feature_value_id, feature_value, record_hash)
            SELECT sd_sid,
            feature_type_id, feature_type, feature_value_id, feature_value, record_hash
            FROM sdcomp.study_features
            where source_id = " + _source_id;

            Execute_SQL(sql_string);
        }


        public void TransferStudyLinks()
        {

            string sql_string = @"INSERT INTO sd.study_links(sd_sid,
            link_label, link_url, record_hash)
            SELECT sd_sid,
            link_label, link_url, record_hash
            FROM sdcomp.study_links
            where source_id = " + _source_id;

            Execute_SQL(sql_string);
        }


        public void TransferStudyCountries()
        {

            string sql_string = @"INSERT INTO sd.study_countries(sd_sid,
            country_id, country_name, status_id, record_hash)
            SELECT sd_sid,
            country_id, country_name, status_id, record_hash
            FROM sdcomp.study_countries
            where source_id = " + _source_id;

            Execute_SQL(sql_string);
        }


        public void TransferStudyLocations()
        {

            string sql_string = @"INSERT INTO sd.study_locations(sd_sid,
            facility_org_id, facility, facility_ror_id, 
            city_id, city_name, country_id, country_name, status_id, record_hash)
            SELECT sd_sid,
            facility_org_id, facility, facility_ror_id, 
            city_id, city_name, country_id, country_name, status_id, record_hash
            FROM sdcomp.study_locations
            where source_id = " + _source_id;

            Execute_SQL(sql_string);
        }


        public void TransferStudyIPDAvaiable()
        {
            string sql_string = @"INSERT INTO sd.study_ipd_available(sd_sid,
            ipd_id, ipd_type, ipd_url, ipd_comment, record_hash)
            SELECT sd_sid,
            ipd_id, ipd_type, ipd_url, ipd_comment, record_hash
            FROM sdcomp.study_ipd_available
            where source_id = " + _source_id;

            Execute_SQL(sql_string);
        }


        public void TransferStudyHashes()
        {
            string sql_string = @"INSERT INTO sd.study_hashes(sd_sid,
            hash_type_id, hash_type, composite_hash)
            SELECT sd_sid,
            hash_type_id, hash_type, composite_hash
            FROM sdcomp.study_hashes
            where source_id = " + _source_id;

            Execute_SQL(sql_string);
        }
    }
}
