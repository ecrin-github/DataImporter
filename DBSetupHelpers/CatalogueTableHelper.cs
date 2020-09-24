using Dapper;
using Npgsql;

namespace DataImporter
{
	class CatalogueTableCreator
	{
		string connstring;

		public CatalogueTableCreator(string _connstring)
		{
			connstring = _connstring;
		}


		public void ExecuteSQL(string sql_string)
		{
			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}

		// These 4 tables ercreatd at each import, but left in the database 
		// until the following import.

		public void CreateStudiesCatTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.studies_catalogue;
            CREATE TABLE ad.studies_catalogue(
                sd_sid                 VARCHAR         NOT NULL PRIMARY KEY
              , status                 INT             NOT NULL
              , study_rec_status       INT             NULL
			);";
			ExecuteSQL(sql_string);
		}


		public void CreateStudyChangedAttsTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.studies_changed_atts;
            CREATE TABLE ad.studies_changed_atts(
                sd_sid                 VARCHAR         NOT NULL PRIMARY KEY
              , hash_type_id           INT             NOT NULL
			);";
			ExecuteSQL(sql_string);
		}


		public void CreateDataObjectsCatTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.objects_catalogue;
            CREATE TABLE ad.objects_catalogue(
                sd_oid                  VARCHAR         NOT NULL PRIMARY KEY
              , status                  INT             NOT NULL
              , object_rec_status       INT             NULL
              , dataset_rec_status      INT             NULL
			);";
			ExecuteSQL(sql_string);
		}


		public void CreateDataObjectsChangedAttsTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.objects_changed_atts;
            CREATE TABLE ad.objects_changed_atts(
                sd_oid                 VARCHAR         NOT NULL PRIMARY KEY
              , hash_type_id           INT             NOT NULL
			);";
			ExecuteSQL(sql_string);
		}
	}


	class CatalogueTableFiller
	{
		string connstring;

		public CatalogueTableFiller(string _connstring)
		{
			connstring = _connstring;
		}

		public void ExecuteSQL(string sql_string)
		{
			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}

		public void IdentifyNewStudies()
		{
			string sql_string = @"INSERT INTO ad.studies_catalogue (sd_sid, status)
                SELECT s.sd_sid, 1 from sd.studies s
                LEFT JOIN ad.studies a
                on s.sd_sid = a.sd_sid 
                WHERE a.sd_sid is null;";
			ExecuteSQL(sql_string);
		}


		public void IdentifyEditedStudies()
		{
			string sql_string = @"INSERT INTO ad.studies_catalogue (sd_sid, status)
			    SELECT s.sd_sid, 2 from sd.studies s
			    INNER JOIN ad.studies a
			    on s.sd_sid = a.sd_sid
                where s.study_full_hash <> a.study_full_hash;";
			ExecuteSQL(sql_string);
		}


		public void IdentifyIdenticalStudies()
		{
			string sql_string = @"INSERT INTO ad.studies_catalogue (sd_sid, status)
				SELECT s.sd_sid, 3 from sd.studies s
				INNER JOIN ad.studies a
				on s.sd_sid = a.sd_sid
                where s.study_full_hash = a.study_full_hash;";
			ExecuteSQL(sql_string);
		}


		public void IdentifyDeletedStudies()
		{
			string sql_string = @"INSERT INTO ad.studies_catalogue(sd_sid, status)
			    SELECT a.sd_sid, 4 from ad.studies a
			    LEFT JOIN sd.studies s
			    on a.sd_sid = s.sd_sid
			    WHERE s.sd_sid is null;";
			ExecuteSQL(sql_string);
		}


		public void IdentifyNewDataObjects()
		{
			string sql_string = @"INSERT INTO ad.objects_catalogue(sd_oid, status)
			    SELECT d.sd_oid, 1 from sd.data_objects d
			    LEFT JOIN ad.data_objects a
                on d.sd_sid = a.sd_sid
                and d.sd_oid = a.sd_oid
			    WHERE a.sd_oid is null;";
			ExecuteSQL(sql_string);
		}


		public void IdentifyEditedDataObjects()
		{
			string sql_string = @"INSERT INTO ad.objects_catalogue(sd_oid, status)
				SELECT d.sd_oid, 2 from sd.data_objects d
				INNER JOIN ad.data_objects a
				on d.sd_sid = a.sd_sid
                and d.sd_oid = a.sd_oid
                WHERE d.object_full_hash <> a.object_full_hash;";
			ExecuteSQL(sql_string);
		}


		public void IdentifyIdenticalDataObjects()
		{
			string sql_string = @"INSERT INTO ad.objects_catalogue(sd_oid, status)
				SELECT d.sd_oid, 3 from sd.data_objects d
				INNER JOIN ad.data_objects a
				on d.sd_sid = a.sd_sid
                and d.sd_oid = a.sd_oid
                WHERE d.object_full_hash = a.object_full_hash;";
			ExecuteSQL(sql_string);
		}


		public void IdentifyDeletedDataObjects()
		{
			string sql_string = @"INSERT INTO ad.objects_catalogue(sd_oid, status)
			SELECT a.sd_oid, 4 from ad.data_objects a
			LEFT JOIN sd.data_objects d
			on a.sd_sid = d.sd_sid
            and a.sd_oid = d.sd_oid
			WHERE d.sd_oid is null;";
			ExecuteSQL(sql_string);
		}

		public void IdentifyChangedStudyRecs()
		{
			string sql_string = @"with t as (
                select s.sd_sid
                from sd.studies s
			    INNER JOIN ad.studies a
			    on s.sd_sid = a.sd_sid
                where s.record_hash <> a.record_hash)
            UPDATE ad.studies_catalogue c
            SET study_rec_status = 2
            from t
			WHERE t.sd_sid = c.sd_sid;";
			ExecuteSQL(sql_string);
		}

		public void IdentifyChangedObjectRecs()
		{
			string sql_string = @"with t as (
                select s.sd_oid
                from sd.data_objects s
			    INNER JOIN ad.data_objects a
			    on s.sd_oid = a.sd_oid
                where s.record_hash <> a.record_hash)
            UPDATE ad.objects_catalogue c
			SET object_rec_status = 2
            FROM t
			WHERE t.sd_oid = c.sd_oid;";
			ExecuteSQL(sql_string);
		}


		public void IdentifyChangedDatasetRecs()
		{
			string sql_string = @"with t as (
                select s.sd_oid
                from sd.dataset_properties s
			    INNER JOIN ad.dataset_properties a
			    on s.sd_oid = a.sd_oid
                where s.record_hash <> a.record_hash)
            UPDATE ad.objects_catalogue c
			SET dataset_rec_status  = 2
            FROM t
			WHERE t.sd_oid = c.sd_oid;";
			ExecuteSQL(sql_string);
		}

		public void IdentifyChangedStudyAtts()
		{
			string sql_string = @"insert into ad.studies_changed_atts
                (sd_sid, hash_type_id)
                select s.sd_sid, s.hash_type_id
                from sd.study_hashes s
			    INNER JOIN ad.study_hashes a
			    on s.sd_sid = a.sd_sid
                and s.hash_type_id = a.hash_type_id
                where s.composite_hash <> a.composite_hash;";
			ExecuteSQL(sql_string);
		}


		public void IdentifyChangedObjectAtts()
		{
			string sql_string = @"insert into ad.objects_changed_atts
                (sd_oid, hash_type_id)
                select s.sd_oid, s.hash_type_id
                from sd.object_hashes s
			    INNER JOIN ad.object_hashes a
                on s.sd_oid = a.sd_oid
			    and s.hash_type_id = a.hash_type_id
                where s.composite_hash <> a.composite_hash;";
			ExecuteSQL(sql_string);
		}


		public ImportEvent CreateImportEvent(int import_id, Source source, bool count_deleted)
		{
			ImportEvent import = new ImportEvent(import_id, source.id);
			string sql_string = "";
			if (source.has_study_tables)
			{
				sql_string = @"select count(*) from ad.studies_catalogue where status = 1;";
				import.num_new_studies = GetScalarDBValue(sql_string);

				sql_string = @"select count(*) from ad.studies_catalogue where status = 2;";
				import.num_edited_studies = GetScalarDBValue(sql_string);

				sql_string = @"select count(*) from ad.studies_catalogue where status = 3;";
				import.num_unchanged_studies = GetScalarDBValue(sql_string);

				if (count_deleted)
                {
					sql_string = @"select count(*) from ad.studies_catalogue where status = 4;";
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

			sql_string = @"select count(*) from ad.objects_catalogue where status = 1;";
			import.num_new_objects = GetScalarDBValue(sql_string);

			sql_string = @"select count(*) from ad.objects_catalogue where status = 2;";
			import.num_edited_objects = GetScalarDBValue(sql_string);

			sql_string = @"select count(*) from ad.objects_catalogue where status = 3;";
			import.num_unchanged_objects = GetScalarDBValue(sql_string);

			if (count_deleted)
			{
				sql_string = @"select count(*) from ad.objects_catalogue where status = 4;";
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
}
