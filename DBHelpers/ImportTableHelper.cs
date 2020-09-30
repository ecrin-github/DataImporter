using Dapper;
using Dapper.Contrib;
using Dapper.Contrib.Extensions;
using Npgsql;

namespace DataImporter
{
	class ImportTableCreator
	{
		string connstring;

		public ImportTableCreator(string _connstring)
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

		public void CreateStudyRecsImportTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.import_study_recs;
            CREATE TABLE ad.import_study_recs(
                sd_sid                 VARCHAR         NOT NULL PRIMARY KEY
              , status                 INT             NOT NULL
              , study_rec_status       INT             NULL
			);";
			ExecuteSQL(sql_string);
		}


		public void CreateStudyAttsImportTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.import_study_changed_atts;
            CREATE TABLE ad.import_study_changed_atts(
                sd_sid                 VARCHAR         NOT NULL
              , hash_type_id           INT             NOT NULL
              , status                 INT             NOT NULL
              , composite_hash         CHAR(32)        NULL
			);";
			ExecuteSQL(sql_string);
		}


		public void CreateObjectRecsImportTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.import_object_recs;
            CREATE TABLE ad.import_object_recs(
                sd_oid                  VARCHAR         NOT NULL PRIMARY KEY
              , status                  INT             NOT NULL
              , object_rec_status       INT             NULL
              , object_dataset_status   INT             NULL
			);";
			ExecuteSQL(sql_string);
		}


		public void CreateObjectAttsImportTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.import_object_changed_atts;
            CREATE TABLE ad.import_object_changed_atts(
                sd_oid                 VARCHAR         NOT NULL
              , hash_type_id           INT             NOT NULL
              , status                 INT             NOT NULL
              , composite_hash         CHAR(32)        NULL
			);";
			ExecuteSQL(sql_string);
		}
	}


	class ImportTableManager
	{
		string connstring;

		public ImportTableManager(string _connstring)
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
			string sql_string = @"INSERT INTO ad.import_study_recs (sd_sid, status)
                SELECT s.sd_sid, 1 from sd.studies s
                LEFT JOIN ad.studies a
                on s.sd_sid = a.sd_sid 
                WHERE a.sd_sid is null;";
			ExecuteSQL(sql_string);
		}


		public void IdentifyEditedStudies()
		{
			string sql_string = @"INSERT INTO ad.import_study_recs (sd_sid, status)
			    SELECT s.sd_sid, 2 from sd.studies s
			    INNER JOIN ad.studies a
			    on s.sd_sid = a.sd_sid
                where s.study_full_hash <> a.study_full_hash;";
			ExecuteSQL(sql_string);
		}


		public void IdentifyIdenticalStudies()
		{
			string sql_string = @"INSERT INTO ad.import_study_recs (sd_sid, status)
				SELECT s.sd_sid, 3 from sd.studies s
				INNER JOIN ad.studies a
				on s.sd_sid = a.sd_sid
                where s.study_full_hash = a.study_full_hash;";
			ExecuteSQL(sql_string);
		}


		public void IdentifyDeletedStudies()
		{
			string sql_string = @"INSERT INTO ad.import_study_recs(sd_sid, status)
			    SELECT a.sd_sid, 4 from ad.studies a
			    LEFT JOIN sd.studies s
			    on a.sd_sid = s.sd_sid
			    WHERE s.sd_sid is null;";
			ExecuteSQL(sql_string);
		}


		public void IdentifyNewDataObjects()
		{
			string sql_string = @"INSERT INTO ad.import_object_recs(sd_oid, status)
			    SELECT d.sd_oid, 1 from sd.data_objects d
			    LEFT JOIN ad.data_objects a
                on d.sd_oid = a.sd_oid
			    WHERE a.sd_oid is null;";
			ExecuteSQL(sql_string);
		}


		public void IdentifyEditedDataObjects()
		{
			string sql_string = @"INSERT INTO ad.import_object_recs(sd_oid, status)
				SELECT d.sd_oid, 2 from sd.data_objects d
				INNER JOIN ad.data_objects a
				on d.sd_oid = a.sd_oid
                WHERE d.object_full_hash <> a.object_full_hash;";
			ExecuteSQL(sql_string);
		}


		public void IdentifyIdenticalDataObjects()
		{
			string sql_string = @"INSERT INTO ad.import_object_recs(sd_oid, status)
				SELECT d.sd_oid, 3 from sd.data_objects d
				INNER JOIN ad.data_objects a
				on d.sd_oid = a.sd_oid
                WHERE d.object_full_hash = a.object_full_hash;";
			ExecuteSQL(sql_string);
		}


		public void IdentifyDeletedDataObjects()
		{
			string sql_string = @"INSERT INTO ad.import_object_recs(sd_oid, status)
			SELECT a.sd_oid, 4 from ad.data_objects a
			LEFT JOIN sd.data_objects d
			on a.sd_oid = d.sd_oid
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
            UPDATE ad.import_study_recs c
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
            UPDATE ad.import_object_recs c
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
            UPDATE ad.import_object_recs c
			SET object_dataset_status = 4
            FROM t
			WHERE t.sd_oid = c.sd_oid;";
			ExecuteSQL(sql_string);
		}


		public void IdentifyChangedStudyAtts()
		{
			// Storeas the sd_id and hash type of all changed composite hash values
			// in edited records - indicates that one or more of thje attributes has changed.

			string sql_string = @"insert into ad.import_study_changed_atts
                (sd_sid, hash_type_id, status, composite_hash)
                select s.sd_sid, s.hash_type_id, 2, s.composite_hash
                from sd.study_hashes s
                INNER JOIN 
                   (SELECT sd_sid FROM ad.import_study_recs 
                    WHERE status = 2) r
                on s.sd_sid = r.sd_sid
			    INNER JOIN ad.study_hashes a
			    on s.sd_sid = a.sd_sid
                and s.hash_type_id = a.hash_type_id
                where s.composite_hash <> a.composite_hash;";
			ExecuteSQL(sql_string);
		}

		public void IdentifyNewStudyAtts()
		{
			// Stores the sd_id and hash type of a new ad_sid / hash type combinations,
			// indicates that one or more of new types of attributes have been added.
			
			string sql_string = @"insert into ad.import_study_changed_atts
			(sd_sid, hash_type_id, status, composite_hash)
                select s.sd_sid, s.hash_type_id, 1, s.composite_hash
                from sd.study_hashes s
                INNER JOIN 
                   (SELECT sd_sid FROM ad.import_study_recs 
                    WHERE status = 2) r
                on s.sd_sid = r.sd_sid
			    LEFT JOIN ad.study_hashes a
			    on s.sd_sid = a.sd_sid
                and s.hash_type_id = a.hash_type_id
                where a.sd_sid is null;";
			ExecuteSQL(sql_string);
		}

		public void IdentifyDeletedStudyAtts()
		{
			// Stores the sd_id and hash type of deleted ad_sid / hash type combinations,
			// indicates that one or more types of attributes have disappeared.
			
			string sql_string = @"insert into ad.import_study_changed_atts
			(sd_sid, hash_type_id, status)
                select a.sd_sid, a.hash_type_id, 4
                from sd.study_hashes s
                INNER JOIN 
                   (SELECT sd_sid FROM ad.import_study_recs 
                    WHERE status = 2) r
                on s.sd_sid = r.sd_sid
			    RIGHT JOIN ad.study_hashes a
			    on a.sd_sid = s.sd_sid
                and a.hash_type_id = s.hash_type_id
                where s.sd_sid is null;";
			ExecuteSQL(sql_string);
		}

		public void IdentifyChangedObjectAtts()
		{
			string sql_string = @"insert into ad.import_object_changed_atts
                (sd_oid, hash_type_id, status, composite_hash)
                select s.sd_oid, s.hash_type_id, 2, s.composite_hash
                from sd.object_hashes s
                INNER JOIN 
                   (SELECT sd_oid FROM ad.import_object_recs 
                    WHERE status = 2) r
                on s.sd_oid = r.sd_oid
			    INNER JOIN ad.object_hashes a
                on s.sd_oid = a.sd_oid
			    and s.hash_type_id = a.hash_type_id
                where s.composite_hash <> a.composite_hash;";

			ExecuteSQL(sql_string);
		}


		public void IdentifyNewObjectAtts()
		{
			string sql_string = @"insert into ad.import_object_changed_atts
                (sd_oid, hash_type_id, status, composite_hash)
                select s.sd_oid, s.hash_type_id, 1, s.composite_hash
                from sd.object_hashes s
                INNER JOIN 
                   (SELECT sd_oid FROM ad.import_object_recs 
                    WHERE status = 2) r
                on s.sd_oid = r.sd_oid
			    LEFT JOIN ad.object_hashes a
			    on s.sd_oid = a.sd_oid
                and s.hash_type_id = a.hash_type_id
                where a.sd_oid is null;";
			ExecuteSQL(sql_string);
		}


		public void IdentifyDeletedObjectAtts()
		{
			string sql_string = @"insert into ad.import_object_changed_atts
                (sd_oid, hash_type_id, status)
                select a.sd_oid, a.hash_type_id, 4
                from sd.object_hashes s
                INNER JOIN 
                   (SELECT sd_oid FROM ad.import_object_recs 
                    WHERE status = 2) r
                on s.sd_oid = r.sd_oid
			    RIGHT JOIN ad.object_hashes a
			    on a.sd_oid = s.sd_oid
                and a.hash_type_id = s.hash_type_id
                where s.sd_oid is null;";
			ExecuteSQL(sql_string);
		}

		public ImportEvent CreateImportEvent(int import_id, Source source, bool count_deleted)
		{
			ImportEvent import = new ImportEvent(import_id, source.id);
			string sql_string = "";
			if (source.has_study_tables)
			{
				sql_string = @"select count(*) from ad.import_study_recs where status = 1;";
				import.num_new_studies = GetScalarDBValue(sql_string);

				sql_string = @"select count(*) from ad.import_study_recs where status = 2;";
				import.num_edited_studies = GetScalarDBValue(sql_string);

				sql_string = @"select count(*) from ad.import_study_recs where status = 3;";
				import.num_unchanged_studies = GetScalarDBValue(sql_string);

				if (count_deleted)
                {
					sql_string = @"select count(*) from ad.import_study_recs where status = 4;";
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

			sql_string = @"select count(*) from ad.import_object_recs where status = 1;";
			import.num_new_objects = GetScalarDBValue(sql_string);

			sql_string = @"select count(*) from ad.import_object_recs where status = 2;";
			import.num_edited_objects = GetScalarDBValue(sql_string);

			sql_string = @"select count(*) from ad.import_object_recs where status = 3;";
			import.num_unchanged_objects = GetScalarDBValue(sql_string);

			if (count_deleted)
			{
				sql_string = @"select count(*) from ad.import_object_recs where status = 4;";
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
