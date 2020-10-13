using Dapper;
using Dapper.Contrib;
using Dapper.Contrib.Extensions;
using Npgsql;

namespace DataImporter
{
	
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

		public void CreateToAggImportTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.to_agg_imports;
            CREATE TABLE ad.to_agg_imports(
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


		public void CreateToAggStudyRecsTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.to_agg_study_recs;
            CREATE TABLE ad.to_agg_study_recs(
                sd_sid                 VARCHAR         NOT NULL PRIMARY KEY
              , status                 INT             NOT NULL
              , study_rec_status       INT             NULL
			);";
			ExecuteSQL(sql_string);
		}

		public void CreateToAggStudyAttsTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.to_agg_study_atts;
            CREATE TABLE ad.to_agg_study_atts(
                sd_sid                 VARCHAR         NOT NULL
              , hash_type_id           INT             NOT NULL
              , status                 INT             NOT NULL
              , composite_hash         CHAR(32)        NULL
			);
            CREATE INDEX to_agg_studyatts_hash ON ad.to_agg_study_atts(hash_type_id);";
			ExecuteSQL(sql_string);
		}


		public void CreateToAggObjectRecsTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.to_agg_object_recs;
            CREATE TABLE ad.to_agg_object_recs(
                sd_oid                  VARCHAR         NOT NULL PRIMARY KEY
              , status                  INT             NOT NULL
              , object_rec_status       INT             NULL
              , object_dataset_status   INT             NULL
			);";
			ExecuteSQL(sql_string);
		}


		public void CreateToAggObjectAttsTable()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.to_agg_object_atts;
            CREATE TABLE ad.to_agg_object_atts(
                sd_oid                 VARCHAR         NOT NULL
              , hash_type_id           INT             NOT NULL
              , status                 INT             NOT NULL
              , composite_hash         CHAR(32)        NULL
			);
            CREATE INDEX to_agg_objectatts_hash ON ad.to_agg_object_atts(hash_type_id);";
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
			// Direct transfer for addition in the aggregation process - 
			// if previous imports also identified new records simply added to the total
			// unless they have been added previously.

			string sql_string = "";
			if (!master_list_has_entries)
			{
				// simply transfer the records
				sql_string = @"INSERT INTO ad.to_agg_study_recs (sd_sid, status)
                SELECT i.sd_sid, i.status from sd.to_ad_study_recs i
                where i.status = 1;";
				ExecuteSQL(sql_string);
			}
			else
			{
				sql_string = @"INSERT INTO ad.to_agg_study_recs (sd_sid, status)
                SELECT i.sd_sid, i.status from sd.to_ad_study_recs i
                left join ad.to_agg_study_recs h
                on i.sd_sid = h.sd_sid
                where i.status = 1
                and h.sd_sid is null;";
				ExecuteSQL(sql_string);
			}

		}

		public void AddStudyRecsDeleted()
		{
			// if a new 'deletion' - unlikely to be possible - add as a new deletion record.
			// Otherwise update the existing record - may be 1, 2, or 3
			// 

			string sql_string = "";

			if (!master_list_has_entries)
			{
				// Simply transfer the records
				sql_string = @"INSERT INTO ad.to_agg_study_recs (sd_sid, status)
			    SELECT i.sd_sid, i.status from sd.to_ad_study_recs i
                where i.status = 4;";
				ExecuteSQL(sql_string);
			}
			else
			{
				// Not sure if this is possible...but just in case

				sql_string = @"INSERT INTO ad.to_agg_study_recs (sd_sid, status)
			    SELECT i.sd_sid, i.status from sd.to_ad_study_recs i
                left join ad.to_agg_study_recs h
                on i.sd_sid = h.sd_sid
                where i.status = 4
                and h.sd_sid is null;";
				ExecuteSQL(sql_string);

				// Deletions on records previously identified
				// (should be the case for all records for deletion)

				sql_string = @"UPDATE ad.to_agg_study_recs h
			    SET status = 4
                FROM sd.to_ad_study_recs i
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
				sql_string = @"INSERT INTO ad.to_agg_study_recs (sd_sid, status, study_rec_status)
			    SELECT i.sd_sid, i.status, i.study_rec_status from sd.to_ad_study_recs i
                where i.status = 2;";
				ExecuteSQL(sql_string);
			}
			else
			{
				// edited records that were 1 earlier remain as 1
				// edited records that were 4 earlier remain as 4
				// edited records that were 3 must be changed to 2
				// and the study_rec_status also transferred

				sql_string = @"UPDATE ad.to_agg_study_recs h
			    SET status = 2,
                study_rec_status = i.study_rec_status
                FROM sd.to_ad_study_recs i
			    where h.sd_sid = i.sd_sid
                and i.status = 2
                and h.status = 3;";
				ExecuteSQL(sql_string);

				// edited records that were 2 stay as 2 (but details of edit may change)
				// if study_rec_status status was 2 it stays as 2
				// if it was 0, now 2, it needs to be changed to reflect that

				sql_string = @"UPDATE ad.to_agg_study_recs h
			    SET study_rec_status = 2
                FROM sd.to_ad_study_recs i
			    where h.sd_sid = i.sd_sid
                and i.status = 2 and h.status = 2
                and i.study_rec_status = 2
                and h.study_rec_status = 0;";
				ExecuteSQL(sql_string);

				// A 2 in the study history rec status therefore indicates 
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
				// but only if the edits refer to records that have been exported
				sql_string = @"INSERT INTO ad.to_agg_study_atts (sd_sid, hash_type_id, status, composite_hash)
                select sd_sid, hash_type_id, status, composite_hash from sd.to_ad_study_atts;";
				ExecuteSQL(sql_string);
			}
			else
			{
				// status = 4, = dropped id / hash type combo - record will already exist so update
				// those with status 1 or 2 - these need to change to 4
				sql_string = @"UPDATE ad.to_agg_study_atts h
                SET status = 4
                FROM sd.to_ad_study_atts i
                WHERE h.sd_sid = i.sd_sid
      			and h.hash_type_id = i.hash_type_id
                and i.status = 4;";
				ExecuteSQL(sql_string);

				// status = 1 or 2 - also add to the table as new records assuming that they do not already exist
				// So add all new records...  Record status is therefore always of the first addition to the table
				// apart from the rare 4 situation

				sql_string = @"INSERT INTO ad.to_agg_study_atts (sd_sid, hash_type_id, status, composite_hash)
                select i.sd_sid, i.hash_type_id, i.status, i.composite_hash from sd.to_ad_study_atts i
                left join ad.to_agg_study_atts h
                on i.sd_sid = h.sd_sid 
                and i.hash_type_id = h.hash_type_id
                where h.sd_sid is null; ";
				ExecuteSQL(sql_string);
			}
		}


		public void AddObjectRecsAdded()
		{
			// direct transfer whether or not previous imports await processing
			// if previous imports also identified new records simply added to the total

			string sql_string = "";
			if (!master_list_has_entries)
			{
				// simply transfer the records
				sql_string = @"INSERT INTO ad.to_agg_object_recs (sd_oid, status)
                SELECT i.sd_oid, i.status from sd.to_ad_object_recs i
                where i.status = 1;";
				ExecuteSQL(sql_string);
			}
			else
			{
				sql_string = @"INSERT INTO ad.to_agg_object_recs(sd_oid, status)
				SELECT i.sd_oid, i.status from sd.to_ad_object_recs i
				left join ad.to_agg_object_recs h
                on i.sd_oid = h.sd_oid
                where i.status = 1
                and h.sd_oid is null;";
				ExecuteSQL(sql_string);
			}
		}


		public void AddObjectRecsDeleted()
		{
			string sql_string = "";

			if (!master_list_has_entries)
			{
				// simply transfer the records
				sql_string = @"INSERT INTO ad.to_agg_object_recs (sd_oid, status)
			    SELECT i.sd_oid, i.status from sd.to_ad_object_recs i
                where i.status = 4;";
				ExecuteSQL(sql_string);
			}
			else
			{
				sql_string = @"INSERT INTO ad.to_agg_object_recs (sd_oid, status)
			    SELECT i.sd_oid, i.status from sd.to_ad_object_recs i
                left join ad.to_agg_object_recs h
                on i.sd_oid = h.sd_oid
                where i.status = 4
                and h.sd_oid is null;";
				ExecuteSQL(sql_string);

				// deletions on records previously identified
				// (should be the case for all records)
				sql_string = @"UPDATE ad.to_agg_object_recs h
			    SET status = 4
                FROM sd.to_ad_object_recs i
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
				sql_string = @"INSERT INTO ad.to_agg_object_recs (sd_oid, status, object_rec_status)
			    SELECT i.sd_oid, i.status, i.object_rec_status from sd.to_ad_object_recs i
                where i.status = 2;";
				ExecuteSQL(sql_string);
			}
			else
			{
				// edited records that were 1 remain as 1
				// edited records that were 4 remain as 4
				// edited records that were 3 must be changed to 2
				// and the study_rec_status also transferred

				sql_string = @"UPDATE ad.to_agg_object_recs h
			    SET status = 2,
                object_rec_status = i.object_rec_status
                FROM sd.to_ad_object_recs i
			    where h.sd_oid = i.sd_oid
                and i.status = 2
                and h.status = 3;";
				ExecuteSQL(sql_string);

				// edited records that were 2 stay as 2 (but details of edit may change)
				// if rec status was 2 it stays as 2
				// if it was 0, now 2, it needs to be changed to reflect that

				sql_string = @"UPDATE ad.to_agg_object_recs h
			    SET object_rec_status = 2
                FROM sd.to_ad_object_recs i
			    where h.sd_oid = i.sd_oid
                and i.status = 2 and h.status = 2
                and i.object_rec_status = 2
                and h.object_rec_status = 0;";
				ExecuteSQL(sql_string);

				// Note here that a 2 in the study rec status indicates 
				// that an edit has taken place in the study record in 
				// any one of the listed imports
				// similarly for the dataset status record...

				sql_string = @"UPDATE ad.to_agg_object_recs h
			    SET object_dataset_status = 4
                FROM sd.to_ad_object_recs i
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
				sql_string = @"INSERT INTO ad.to_agg_object_atts (sd_oid, hash_type_id, status, composite_hash)
                select sd_oid, hash_type_id, status, composite_hash from sd.to_ad_object_atts;";
				ExecuteSQL(sql_string);
			}
			else
			{
				// status = 4, = dropped id / hash type combo - added as new record to table where it 
				// does not exist unless was earlier put in as 1 or 2 - then these need to change to 4
				sql_string = @"UPDATE ad.to_agg_object_atts h
                SET status = 4
                FROM sd.to_ad_object_atts i
                WHERE h.sd_oid = i.sd_oid
      			and h.hash_type_id = i.hash_type_id
                and i.status = 4;";
				ExecuteSQL(sql_string);

				// status = 1 or 2 - also add to the table as new records assuming that they do not already exist
				// So add all new records...  Record status is therfore always of the first addition to the table
				// apart from the rare 4 situation
                sql_string = @"INSERT INTO ad.to_agg_object_atts (sd_oid, hash_type_id, status, composite_hash)
                select i.sd_oid, i.hash_type_id, i.status, i.composite_hash from sd.to_ad_object_atts i
                left join ad.to_agg_object_atts h
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
			string sql_string = @"Select count(*) from ad.to_agg_imports";
			
			using (var Conn = new NpgsqlConnection(connstring))
			{
				int res = Conn.ExecuteScalar<int>(sql_string);
				master_list_has_entries = (res > 0);
			}
		}


		public ImportEvent CreateImportEvent(int import_id, Source source)
		{
			ImportEvent import = new ImportEvent(import_id, source.id);
			string sql_string = "";
			if (source.has_study_tables)
			{
				sql_string = @"select count(*) from sd.to_ad_study_recs where status = 1;";
				import.num_new_studies = GetScalarDBValue(sql_string);

				sql_string = @"select count(*) from sd.to_ad_study_recs where status = 2;";
				import.num_edited_studies = GetScalarDBValue(sql_string);

				sql_string = @"select count(*) from sd.to_ad_study_recs where status = 3;";
				import.num_unchanged_studies = GetScalarDBValue(sql_string);

				sql_string = @"select count(*) from sd.to_ad_study_recs where status = 4;";
				import.num_deleted_studies = GetScalarDBValue(sql_string);
			}
			else
			{
				import.num_new_studies = 0;
				import.num_edited_studies = 0;
				import.num_unchanged_studies = 0;
				import.num_deleted_studies = 0;
			}

			sql_string = @"select count(*) from sd.to_ad_object_recs where status = 1;";
			import.num_new_objects = GetScalarDBValue(sql_string);

			sql_string = @"select count(*) from sd.to_ad_object_recs where status = 2;";
			import.num_edited_objects = GetScalarDBValue(sql_string);

			sql_string = @"select count(*) from sd.to_ad_object_recs where status = 3;";
			import.num_unchanged_objects = GetScalarDBValue(sql_string);

			sql_string = @"select count(*) from sd.to_ad_object_recs where status = 4;";
			import.num_deleted_objects = GetScalarDBValue(sql_string);

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
