using Dapper;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataImporter.DBHelpers
{
	class TempTableCreator
	{
		string db_conn;

		public TempTableCreator(string _db_conn)
		{
			db_conn = _db_conn;
		}

		public void CreateNewStudiesTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.temp_new_studies;
            CREATE TABLE ad.temp_new_studies(
                sd_id                  VARCHAR         NOT NULL PRIMARY KEY
			  , study_hash_id          CHAR(32)        NOT NULL
              , study_ad_id            INT             NULL
			);";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void CreateNewDataObjectsTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.temp_new_data_objects;
            CREATE TABLE IF NOT EXISTS ad.temp_new_data_objects(
                sd_id                  VARCHAR         NOT NULL 
              , do_id                  INT             NOT NULL
			  , object_hash_id         CHAR(32)        NOT NULL
              , object_ad_id           INT             NULL,
              PRIMARY KEY (sd_id, do_id)
			);";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void CreateMatchedStudiesTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.temp_matched_studies;
            CREATE TABLE IF NOT EXISTS ad.temp_matched_studies(
                sd_id                  VARCHAR         NOT NULL PRIMARY KEY
			  , study_hash_id          CHAR(32)        NOT NULL
              , study_ad_id            INT             NULL
			);";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void CreateMissingStudiesTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.temp_missing_studies;     
            CREATE TABLE IF NOT EXISTS ad.temp_missing_studies(
                sd_id                  VARCHAR         NOT NULL PRIMARY KEY
			  , study_hash_id          CHAR(32)        NOT NULL
              , study_ad_id            INT             NULL
			);";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}
	}


	class TempTableFiller
	{
		string db_conn;

		public TempTableFiller(string _db_conn)
		{
			db_conn = _db_conn;
		}

		public void FillNewStudiesTable()
		{
			string sql_string = @"INSERT INTO ad.temp_new_studies (sd_id, study_hash_id)
            SELECT s.sd_id, s.hash_id from sd.studies s
            LEFT JOIN ad.studies a
            on s.sd_id = a.sd_id 
            WHERE a.sd_id is null;";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void FillNewDataObjectsTable()
		{
			string sql_string = @"INSERT INTO ad.temp_new_data_objects(sd_id, do_id, object_hash_id)
			SELECT d.sd_id, d.do_id, d.object_hash_id from sd.studies s
			LEFT JOIN ad.studies a
			on s.sd_id = a.sd_id
            INNER JOIN sd.data_objects d
            on s.sd_id = d.sd_id
			WHERE a.sd_id is null;";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void FillMatchedStudiesTable()
		{
			string sql_string = @"INSERT INTO ad.temp_matched_studies (sd_id, study_hash_id)
            SELECT s.sd_id, s.hash_id from sd.studies s
            INNER JOIN ad.studies a
            on s.sd_id = a.sd_id;";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void FillMissingStudiesTable()
		{
			string sql_string = @"INSERT INTO ad.temp_missing_studies(sd_id, study_hash_id)
			SELECT a.sd_id, a.hash_id from ad.studies a
			LEFT JOIN sd.studies s
			on a.sd_id = s.sd_id
			WHERE s.sd_id is null;";

			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

	}


	class TempTableDropper
	{
		string db_conn;

		public TempTableDropper(string _db_conn)
		{
			db_conn = _db_conn;
		}

		public void DeleteNewStudiesTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.temp_new_studies;";
			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void DeleteNewDataObjectsTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.temp_new_data_objects;";
			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void DeleteMatchedStudiesTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.temp_matched_studies;";
			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void DeleteMissingStudiesTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.temp_missing_studies;";
			using (var conn = new Npgsql.NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}
	}

}
