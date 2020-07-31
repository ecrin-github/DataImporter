using Dapper;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataImporter
{
	class TempTableCreator
	{
		string connstring;

		public TempTableCreator(string _connstring)
		{
			connstring = _connstring;
		}


		public void CreateTempStudiesTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.temp_studies;
            CREATE TABLE ad.temp_studies(
                sd_sid                 VARCHAR         NOT NULL PRIMARY KEY
              , status                 INT             NOT NULL
			);";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}

		public void CreateTempDataObjectsTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.temp_data_objects;
            CREATE TABLE IF NOT EXISTS ad.temp_data_objects(
                sd_oid                  VARCHAR         NOT NULL PRIMARY KEY
              , sd_sid                  VARCHAR         NOT NULL
              , status                  INT             NOT NULL
			);";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}

	}


	class TempTableFiller
	{
		string connstring;

		public TempTableFiller(string _connstring)
		{
			connstring = _connstring;
		}

		public void IdentifyNewStudies()
		{
			string sql_string = @"INSERT INTO ad.temp_studies (sd_sid, status)
            SELECT s.sd_sid, 1 from sd.studies s
            LEFT JOIN ad.studies a
            on s.sd_sid = a.sd_sid 
            WHERE a.sd_sid is null;";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void IdentifyEditedStudies()
		{
			string sql_string = @"INSERT INTO ad.temp_studies (sd_sid, status)
			SELECT s.sd_sid, 2 from sd.studies s
			INNER JOIN ad.studies a
			on s.sd_sid = a.sd_sid
            where s.study_full_hash <> a.study_full_hash;";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}

		}


		public void IdentifyIdenticalStudies()
		{
			string sql_string = @"INSERT INTO ad.temp_studies (sd_sid, status)
				SELECT s.sd_sid, 3 from sd.studies s
				INNER JOIN ad.studies a
				on s.sd_sid = a.sd_sid
                where s.study_full_hash = a.study_full_hash;";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}
		

		public void IdentifyDeletedStudies()
		{
			string sql_string = @"INSERT INTO ad.temp_studies(sd_sid, status)
			SELECT a.sd_sid, 4 from ad.studies a
			LEFT JOIN sd.studies s
			on a.sd_sid = s.sd_sid
			WHERE s.sd_id is null;";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void UpdateFullStudyHash()
		{
			// Also ensure study_full_hash is updated to reflect new value
			// The study record itself may not have changed, so a study
			// record update cannot be used to make the edit 

			string sql_string = @"UPDATE ad.studies a
			  set study_full_hash = s.study_full_hash
              FROM sd.studies s
			  WHERE a.sd_sid = a.sd_sid
              AND s.study_full_hash <> a.study_full_hash;";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void IdentifyNewDataObjects()
		{
			string sql_string = @"INSERT INTO ad.temp_data_objects(sd_oid, sd_sid, status)
			SELECT d.sd_oid, d.do_oid, 1 from sd.data_objects d
			LEFT JOIN ad.data_objects a
			on d.sd_sid = a.sd_sid
            and d.sd_oid = a.sd_oid
			WHERE a.sd_oid is null;";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void IdentifyEditedDataObjects()
		{
			string sql_string = @"INSERT INTO ad.temp_data_objects(sd_oid, sd_sid, status)
				SELECT d.sd_oid, d.do_oid, 2 from sd.data_objects d
				INNER JOIN ad.data_objects a
				on d.sd_sid = a.sd_sid
                and d.sd_oid = a.sd_oid
                WHERE d.object_full_hash <> a.object_full_hash;";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}

			// ensure object_full_hash is updated to reflect new value
			// The object record itself may not have changed, so an object
			// record update cannot be used to make the edit 
		}


		public void IdentifyIdenticalDataObjects()
		{
			string sql_string = @"INSERT INTO ad.temp_data_objects(sd_oid, sd_sid, status)
				SELECTd.sd_oid, d.do_oid, 3 from sd.data_objects d
				INNER JOIN ad.data_objects a
				on d.sd_sid = a.sd_sid
                and d.sd_oid = a.sd_oid
                WHERE d.object_full_hash = a.object_full_hash;";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void IdentifyDeletedDataObjects()
		{
			string sql_string = @"INSERT INTO ad.temp_data_objects(sd_oid, sd_sid, status)
			SELECT a.sd_oid, a.do_oid, 4 from ad.data_objects a
			LEFT JOIN sd.data_objects d
			on a.sd_sid = d.sd_sid
            and a.sd_oid = d.sd_oid
			WHERE d.sd_oid is null;";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void UpdateFullObjectHash()
		{
			// Also ensure study_full_hash is updated to reflect new value
			// The study record itself may not have changed, so a study
			// record update cannot be used to make the edit 

			string sql_string = @"UPDATE ad.data_objects a
			  set object_full_hash = s.object_full_hash
              FROM sd.data_objects s
			  WHERE a.sd_sid = a.sd_sid
              AND a.sd_oid = a.sd_oid
              AND s.object_full_hash <> a.object_full_hash;";

			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}
	}


	class TempTableDropper
	{
		string connstring;

		public TempTableDropper(string _connstring)
		{
			connstring = _connstring;
		}

		public void DeleteTempStudiesTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.temp_studies;";
			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}

		public void TempDataObjectsTable()
{
			string sql_string = @"DROP TABLE IF EXISTS ad.temp_data_objects;";
			using (var conn = new Npgsql.NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}
	}

}
