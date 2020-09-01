using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataImporter
{
	class StudyDataDeleter
	{
		string connstring;

		public StudyDataDeleter(string _connstring)
		{
			connstring = _connstring;
		}


		public void DeleteStudies()
		{
			// if the record hash for the study has changed, then the data in the studies records should be changed

			string sql_string = @"with t as (
			      select * from ad.temp_studies
			      where status = 4)
			  delete from ad.studies
              using t
			  where sd_sid = t.sd_sid;";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void UpdateStudiesLastImportedDate(int last_import_id, int source_id)
		{
			string sql_string = @"Update sf.source_data_studies s
            set last_import_id = " + last_import_id.ToString() + @", 
            last_imported = current_timestamp
            from ad.temp_studies ts
            where s.sd_sid = ts.sd_sid and
            s.source_id = " + source_id.ToString() + @"
			and ts.status = 2";


			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}

		}


		public void DeleteStudyIdentifiers()
		{
			// Where the composite hash value indicates that a change has taken place in one or more 
			// of the records replace the whole set for the relevant studies
			// Composite hash id for study identifiers = 11

			string sql_string = @"with t as (
			      select * from ad.temp_studies
			      where status = 4)
			  delete from ad.study_identifiers
              using t
			  where sd_sid = t.sd_sid;";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}

			
		}


		public void DeleteStudyTitles()
		{

			// Where the composite hash value indicates that a change has taken place in one or more 
			// of the records replace the whole set for the relevant studies
			// Composite hash id for study titles = 12

			string sql_string = @"with t as (
			      select * from ad.temp_studies
			      where status = 4)
			  delete from ad.study_titles
              using t
			  where sd_sid = t.sd_sid;";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void DeleteStudyReferences()
		{
			// Where the composite hash value indicates that a change has taken place in one or more 
			// of the records replace the whole set for the relevant studies
			// Composite hash id for study references = 17

			string sql_string = @"with t as (
			      select * from ad.temp_studies
			      where status = 4)
			  delete from ad.study_references
              using t
			  where sd_sid = t.sd_sid;";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}

		public void DeleteStudyContributors()
		{
			// Where the composite hash value indicates that a change has taken place in one or more 
			// of the records replace the whole set for the relevant studies
			// Composite hash id for study contributors = 15

			string sql_string = @"with t as (
			      select * from ad.temp_studies
			      where status = 4)
			  delete from ad.study_contributors
              using t
			  where sd_sid = t.sd_sid;";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void DeleteStudyTopics()
		{
			// Where the composite hash value indicates that a change has taken place in one or more 
			// of the records replace the whole set for the relevant studies
			// Composite hash id for study topics = 14

			string sql_string = @"with t as (
			      select * from ad.temp_studies
			      where status = 4)
			  delete from ad.study_topics
              using t
			  where sd_sid = t.sd_sid;";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void DeleteStudyRelationships()
		{
			// Where the composite hash value indicates that a change has taken place in one or more 
			// of the records replace the whole set for the relevant studies
			// Composite hash id for study relationships = 16

			string sql_string = @"with t as (
			      select * from ad.temp_studies
			      where status = 4)
			  delete from ad.study_relationships
              using t
			  where sd_sid = t.sd_sid;";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void DeleteStudyFeatures()
		{
			// Where the composite hash value indicates that a change has taken place in one or more 
			// of the records replace the whole set for the relevant studies
			// Composite hash id for study features = 13

			string sql_string = @"with t as (
			      select * from ad.temp_studies
			      where status = 4)
			  delete from ad.study_features
              using t
			  where sd_sid = t.sd_sid;";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void DeleteStudyLinks()
		{
			// Where the composite hash value indicates that a change has taken place in one or more 
			// of the records replace the whole set for the relevant studies
			// Composite hash id for study links = ???

			string sql_string = @"with t as (
			      select * from ad.temp_studies
			      where status = 4)
			  delete from ad.study_links
              using t
			  where sd_sid = t.sd_sid;";
			
			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void DeleteStudyIpdAvailable()
		{
			// Where the composite hash value indicates that a change has taken place in one or more 
			// of the records replace the whole set for the relevant studies
			// Composite hash id for study ipd available = ???

			string sql_string = @"with t as (
			      select * from ad.temp_studies
			      where status = 4)
			  delete from ad.study_ipd_available
              using t
			  where sd_sid = t.sd_sid;";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void DeleteStudyHashes()
		{

			string sql_string = @"with t as (
			      select * from ad.temp_studies
			      where status = 4)
			  delete from ad.study_hashes
              using t
			  where sd_sid = t.sd_sid;";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}
	}
}
