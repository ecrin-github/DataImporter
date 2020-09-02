using Dapper;
using Npgsql;

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

			using (var conn = new NpgsqlConnection(connstring))
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

			using (var conn = new NpgsqlConnection(connstring))
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

			using (var conn = new NpgsqlConnection(connstring))
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

			using (var conn = new NpgsqlConnection(connstring))
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

			using (var conn = new NpgsqlConnection(connstring))
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
			WHERE s.sd_sid is null;";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void IdentifyNewDataObjects()
		{
			string sql_string = @"INSERT INTO ad.temp_data_objects(sd_oid, sd_sid, status)
			SELECT d.sd_oid, d.sd_sid, 1 from sd.data_objects d
			LEFT JOIN ad.data_objects a
			on d.sd_sid = a.sd_sid
            and d.sd_oid = a.sd_oid
			WHERE a.sd_oid is null;";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void IdentifyEditedDataObjects()
		{
			string sql_string = @"INSERT INTO ad.temp_data_objects(sd_oid, sd_sid, status)
				SELECT d.sd_oid, d.sd_sid, 2 from sd.data_objects d
				INNER JOIN ad.data_objects a
				on d.sd_sid = a.sd_sid
                and d.sd_oid = a.sd_oid
                WHERE d.object_full_hash <> a.object_full_hash;";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void IdentifyIdenticalDataObjects()
		{
			string sql_string = @"INSERT INTO ad.temp_data_objects(sd_oid, sd_sid, status)
				SELECT d.sd_oid, d.sd_sid, 3 from sd.data_objects d
				INNER JOIN ad.data_objects a
				on d.sd_sid = a.sd_sid
                and d.sd_oid = a.sd_oid
                WHERE d.object_full_hash = a.object_full_hash;";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public void IdentifyDeletedDataObjects()
		{
			string sql_string = @"INSERT INTO ad.temp_data_objects(sd_oid, sd_sid, status)
			SELECT a.sd_oid, a.sd_sid, 4 from ad.data_objects a
			LEFT JOIN sd.data_objects d
			on a.sd_sid = d.sd_sid
            and a.sd_oid = d.sd_oid
			WHERE d.sd_oid is null;";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


		public ImportEvent CreateImportEvent(int import_id, Source source, bool count_deleted)
		{
			ImportEvent import = new ImportEvent(import_id, source.id);
			string sql_string = "";
			if (source.has_study_tables)
			{
				sql_string = @"select count(*) from ad.temp_studies where status = 1;";
				import.num_new_studies = GetScalarDBValue(sql_string);

				sql_string = @"select count(*) from ad.temp_studies where status = 2;";
				import.num_edited_studies = GetScalarDBValue(sql_string);

				sql_string = @"select count(*) from ad.temp_studies where status = 3;";
				import.num_unchanged_studies = GetScalarDBValue(sql_string);

				if (count_deleted)
                {
					sql_string = @"select count(*) from ad.temp_studies where status = 4;";
					import.num_deleted_studies = GetScalarDBValue(sql_string);
				}
				else
                {
					import.num_deleted_studies = 0;
				}
			}
			else
			{
				import.num_new_studies = 0;
				import.num_edited_studies = 0;
				import.num_unchanged_studies = 0;
				import.num_deleted_studies = 0;
			}

			sql_string = @"select count(*) from ad.temp_data_objects where status = 1;";
			import.num_new_objects = GetScalarDBValue(sql_string);

			sql_string = @"select count(*) from ad.temp_data_objects where status = 2;";
			import.num_edited_objects = GetScalarDBValue(sql_string);

			sql_string = @"select count(*) from ad.temp_data_objects where status = 3;";
			import.num_unchanged_objects = GetScalarDBValue(sql_string);

			if (count_deleted)
			{
				sql_string = @"select count(*) from ad.temp_data_objects where status = 4;";
				import.num_deleted_objects = GetScalarDBValue(sql_string);
			}
			else
			{
				import.num_deleted_objects = 0;
			}

			return import;
		}


		private int GetScalarDBValue(string sql_string)
        {
			using (var Conn = new NpgsqlConnection(connstring))
			{
				return Conn.ExecuteScalar<int>(sql_string);
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
