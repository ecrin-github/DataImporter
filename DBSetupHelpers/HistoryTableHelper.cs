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
                sd_sid                 VARCHAR         NOT NULL PRIMARY KEY
              , hash_type_id           INT             NOT NULL
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
                sd_oid                 VARCHAR         NOT NULL PRIMARY KEY
              , hash_type_id           INT             NOT NULL
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
			string sql_string = @"insert into ad.import_study_changed_atts
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
			string sql_string = @"insert into ad.import_object_changed_atts
                (sd_oid, hash_type_id)
                select s.sd_oid, s.hash_type_id
                from sd.object_hashes s
			    INNER JOIN ad.object_hashes a
                on s.sd_oid = a.sd_oid
			    and s.hash_type_id = a.hash_type_id
                where s.composite_hash <> a.composite_hash;";
			ExecuteSQL(sql_string);

			sql_string = @"with t as (
                select distinct sd_oid
                from ad.import_object_changed_atts)
            UPDATE ad.import_object_recs c
			SET object_rec_status = object_rec_status + 1
            FROM t
			WHERE t.sd_oid = c.sd_oid;";
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

	class HistoryTableCreator
	{
		string connstring;

		public HistoryTableCreator(string _connstring)
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

		public void CreateHistoryMasterTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.history_master_list;
            CREATE TABLE ad.history_master_list(
                id                     INT             NOT NULL
              , num_new_studies        INT             NULL
              , num_edited_studies     INT             NULL
              , num_unchanged_studies  INT             NULL
              , num_deleted_studies    INT             NULL
              , num_new_objects        INT             NULL
              , num_edited_objects     INT             NULL
              , num_unchanged_objects  INT             NULL
              , num_deleted_objects    INT             NULL
              , time_created           TIMESTAMPTZ     NULL
			);";
			ExecuteSQL(sql_string);
		}


		public void CreateStudyRecsHistoryTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.history_study_recs;
            CREATE TABLE ad.history_study_recs(
                sd_sid                 VARCHAR         NOT NULL PRIMARY KEY
              , status                 INT             NOT NULL
              , study_rec_status       INT             NULL
			);";
			ExecuteSQL(sql_string);
		}


		public void CreateStudyAttsHistoryTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.history_study_changed_atts;
            CREATE TABLE ad.history_study_changed_atts(
                sd_sid                 VARCHAR         NOT NULL PRIMARY KEY
              , hash_type_id           INT             NOT NULL
			);";
			ExecuteSQL(sql_string);
		}


		public void CreateObjectRecsHistoryTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.history_object_recs;
            CREATE TABLE ad.history_object_recs(
                sd_oid                  VARCHAR         NOT NULL PRIMARY KEY
              , status                  INT             NOT NULL
              , object_rec_status       INT             NULL
              , object_dataset_status   INT             NULL
			);";
			ExecuteSQL(sql_string);
		}


		public void CreateObjectAttsHistoryTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.history_object_changed_atts;
            CREATE TABLE ad.history_object_changed_atts(
                sd_oid                 VARCHAR         NOT NULL PRIMARY KEY
              , hash_type_id           INT             NOT NULL
			);";
			ExecuteSQL(sql_string);
		}
	}


	class HistoryTableManager
	{
		string connstring;
		bool master_list_has_entries;

		public HistoryTableManager(string _connstring)
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

		public void AddStudyRecsAdded()
		{
			// direct transfer whether or not previous imports await processing
			// if previous imports also identified new records simply added to the total

			string sql_string = @"INSERT INTO ad.history_study_recs (sd_sid, status)
                SELECT i.sd_sid, i.status from ad.import_study_recs i
                where i.status = 1;";
			ExecuteSQL(sql_string);
		}


		public void AddStudyRecsDeleted()
		{
			// if a 'deletion' - unlikely to be possible - add as a new deletion record.
			// Otherwise update the existing record - may be 1, 2, or 3
			// 

			string sql_string = "";

			if (!master_list_has_entries)
			{
				// simply transfer the records
				sql_string = @"INSERT INTO ad.history_study_recs (sd_sid, status)
			    SELECT i.sd_sid, i.status from ad.import_study_recs i
                where i.status = 4;";
				ExecuteSQL(sql_string);
			}
			else
			{
				sql_string = @"INSERT INTO ad.history_study_recs (sd_sid, status)
			    SELECT i.sd_sid, i.status from ad.import_study_recs i
                left join ad.history_study_recs h
                on i.sd_sid = h.sd_sid
                where i.status = 4
                and h.sd_sid is null;";
				ExecuteSQL(sql_string);

				// deletions on records previously identified
				// (should be the case for all records)
				sql_string = @"UPDATE ad.history_study_recs h
			    SET status = 4
                FROM ad.import_study_recs i
			    where h.sd_sid = i.sd_sid
                and h.status = 4;";
				ExecuteSQL(sql_string);
			}
		}


		public void ProcessEditedStudyRecs()
		{
			// for edited records  - change the status of previously
			// matched records (3 to 2)
			// records that were 1, 2 will remain 1 or 2 but for previously 
			// edited records the addtional edit actions need to be added in
			
			string sql_string = "";

			if (!master_list_has_entries)
			{
				// simply transfer the records
				sql_string = @"INSERT INTO ad.history_study_recs (sd_sid, status, study_rec_status)
			    SELECT i.sd_sid, i.status, i.study_rec_status from ad.import_study_recs i
                where i.status = 2;";
				ExecuteSQL(sql_string);
			}
			else
			{
				// edited records that were 1 remain as 1
				// edited records that were 4 remain as 4
				// edited records that were 3 must be changed to 2
				// and the study_rec_status also transferred

				sql_string = @"UPDATE ad.history_study_recs h
			    SET status = 2,
                study_rec_status = i.study_rec_status
                FROM ad.import_study_recs i
			    where h.sd_sid = i.sd_sid
                and i.status = 2
                and h.status = 3;";
				ExecuteSQL(sql_string);

				// edited records that were 2 stay as 2 (but details of edit may change)

				// if rec status was 2 it stays as 2
				// if it was 0, now 2, it needs to be changed to reflect that
				sql_string = @"UPDATE ad.history_study_recs h
			    SET study_rec_status = 2
                FROM ad.import_study_recs i
			    where h.sd_sid = i.sd_sid
                and i.status = 2 and h.status = 2
                and i.study_rec_status = 2
                and h.study_rec_status = 0;";
				ExecuteSQL(sql_string);

				// Note here that a 2 in the study rec status indicates 
				// that an edit has taken place in the study record in 
				// any one of the listed imports
			}
		}


		public void ProcessEditedStudyAtts()
		{
			string sql_string = "";

			if (!master_list_has_entries)
			{
				// simply transfer the records
				sql_string = @"INSERT INTO ad.history_study_changed_atts (sd_sid, hash_type_id)
                select sd_sid, hash_type_id from ad.import_study_changed_atts;";
				ExecuteSQL(sql_string);
			}
			else
			{
				// previous imports recorded - accumulate the att changes by adding the 
				// additional ones in the new import
				sql_string = @"INSERT INTO ad.history_study_changed_atts (sd_sid, hash_type_id)
                select i.sd_sid, i.hash_type_id from ad.import_study_changed_atts i
                left join ad.history_study_changed_atts h
                on i.sd_sid = h.sd_sid 
                and i.hash_type_id = h.hash_type_id
                where h.sd_sid is null;"; 
				ExecuteSQL(sql_string);
			}
		}



		public void AddObjectRecsAdded()
		{
			// direct transfer whether or not previous imports await processing
			// if previous imports also identified new records simply added to the total

			string sql_string = @"INSERT INTO ad.history_object_recs (sd_oid, status)
                SELECT i.sd_oid, i.status from ad.import_object_recs i
                where i.status = 1;";
			ExecuteSQL(sql_string);
		}


		public void AddObjectRecsDeleted()
		{
			string sql_string = "";

			if (!master_list_has_entries)
			{
				// simply transfer the records
				sql_string = @"INSERT INTO ad.history_object_recs (sd_oid, status)
			    SELECT i.sd_oid, i.status from ad.import_object_recs i
                where i.status = 4;";
				ExecuteSQL(sql_string);
			}
			else
			{
				sql_string = @"INSERT INTO ad.history_object_recs (sd_oid, status)
			    SELECT i.sd_oid, i.status from ad.import_object_recs i
                left join ad.history_object_recs h
                on i.sd_oid = h.sd_oid
                where i.status = 4
                and h.sd_oid is null;";
				ExecuteSQL(sql_string);

				// deletions on records previously identified
				// (should be the case for all records)
				sql_string = @"UPDATE ad.history_object_recs h
			    SET status = 4
                FROM ad.import_object_recs i
			    where h.sd_oid = i.sd_oid
                and i.status = 4;";
				ExecuteSQL(sql_string);
			}
		}


		public void ProcessEditedObjectRecs()
		{
     		string sql_string = "";

			if (!master_list_has_entries)
			{
				// simply transfer the records
				sql_string = @"INSERT INTO ad.history_object_recs (sd_oid, status, object_rec_status)
			    SELECT i.sd_oid, i.status, i.object_rec_status from ad.import_object_recs i
                where i.status = 2;";
				ExecuteSQL(sql_string);
			}
			else
			{
				// edited records that were 1 remain as 1
				// edited records that were 4 remain as 4
				// edited records that were 3 must be changed to 2
				// and the study_rec_status also transferred

				sql_string = @"UPDATE ad.history_object_recs h
			    SET status = 2,
                object_rec_status = i.object_rec_status
                FROM ad.import_object_recs i
			    where h.sd_oid = i.sd_oid
                and i.status = 2
                and h.status = 3;";
				ExecuteSQL(sql_string);

				// edited records that were 2 stay as 2 (but details of edit may change)
				// if rec status was 2 it stays as 2
				// if it was 0, now 2, it needs to be changed to reflect that

				sql_string = @"UPDATE ad.history_object_recs h
			    SET object_rec_status = 2
                FROM ad.import_object_recs i
			    where h.sd_oid = i.sd_oid
                and i.status = 2 and h.status = 2
                and i.object_rec_status = 2
                and h.object_rec_status = 0;";
				ExecuteSQL(sql_string);

				// Note here that a 2 in the study rec status indicates 
				// that an edit has taken place in the study record in 
				// any one of the listed imports
				// similarly for the dataset status record...

				sql_string = @"UPDATE ad.history_object_recs h
			    SET object_dataset_status = 4
                FROM ad.import_object_recs i
			    where h.sd_oid = i.sd_oid
                and i.status = 2 and h.status = 2
                and i.object_dataset_status = 4
                and h.object_dataset_status = 0;";
				ExecuteSQL(sql_string);
			}
		}

		public void ProcessEditedObjectAtts()
		{
			string sql_string = "";

			if (!master_list_has_entries)
			{
				// simply transfer the records
				sql_string = @"INSERT INTO ad.history_object_changed_atts (sd_oid, hash_type_id)
                select sd_oid, hash_type_id from ad.import_object_changed_atts;";
				ExecuteSQL(sql_string);
			}
			else
			{
				// previous imports recorded - accumulate the att changes by adding the 
				// additional ones in the new import
				sql_string = @"INSERT INTO ad.history_object_changed_atts (sd_oid, hash_type_id)
                select i.sd_oid, i.hash_type_id from ad.import_object_changed_atts i
                left join ad.history_object_changed_atts h
                on i.sd_oid = h.sd_oid 
                and i.hash_type_id = h.hash_type_id
                where h.sd_oid is null;";
				ExecuteSQL(sql_string);
			}
		}


		public void CreateAndStoreHistoryRecord(ImportEvent import)
		{
			HistoryRecord record = new HistoryRecord(import);

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Insert<HistoryRecord>(record);
			}
		}


		public void ObtainMasterListState()
		{
			string sql_string = @"Select count(*) from ad.history_master_list";
			
			using (var Conn = new NpgsqlConnection(connstring))
			{
				int res = Conn.ExecuteScalar<int>(sql_string);
				master_list_has_entries = (res > 0);
			}
		}
	}
}
