﻿using Dapper.Contrib.Extensions;
using Dapper;
using Npgsql;
using System;
using Microsoft.Extensions.Configuration;
using System.Linq;
using System.Collections.Generic;

namespace DataImporter
{
	public class LoggingDataLayer
	{
		private string connString;
		private Source source;
		private string sql_file_select_string;


		/// <summary>
		/// Parameterless constructor is used to automatically build
		/// the connection string, using an appsettings.json file that 
		/// has the relevant credentials (but which is not stored in GitHub).
		/// </summary>
		/// 
		public LoggingDataLayer()
		{
			IConfigurationRoot settings = new ConfigurationBuilder()
				.SetBasePath(AppContext.BaseDirectory)
				.AddJsonFile("appsettings.json")
				.Build();

			NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
			builder.Host = settings["host"];
			builder.Username = settings["user"];
			builder.Password = settings["password"];

			builder.Database = "mon";
			connString = builder.ConnectionString;

			sql_file_select_string = "select id, source_id, sd_id, remote_url, last_revised, ";
			sql_file_select_string += " assume_complete, download_status, local_path, last_saf_id, last_downloaded, ";
			sql_file_select_string += " last_harvest_id, last_harvested, last_import_id, last_imported ";

		}

		public Source SourceParameters => source;

		public Source FetchSourceParameters(int source_id)
		{
			using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
			{
				source = Conn.Get<Source>(source_id);
				return source;
			}
		}

		public int GetNextImportEventId()
		{
			using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
			{
				string sql_string = "select max(id) from sf.import_events ";
				int last_id = Conn.ExecuteScalar<int>(sql_string);
				return last_id + 1;
			}


		}
		public IEnumerable<StudyFileRecord> FetchStudyFileRecords(int source_id, int harvest_type_id = 1, DateTime? cutoff_date = null)
		{
			string sql_string = sql_file_select_string;
			sql_string += " from sf.source_data_studies ";
			sql_string += GetWhereClause(source_id, harvest_type_id, cutoff_date);

			using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
			{
				return Conn.Query<StudyFileRecord>(sql_string);
			}
		}

		public IEnumerable<ObjectFileRecord> FetchObjectFileRecords(int source_id, int harvest_type_id = 1, DateTime? cutoff_date = null)
		{
			string sql_string = sql_file_select_string;
			sql_string += " from sf.source_data_objects";
			sql_string += GetWhereClause(source_id, harvest_type_id, cutoff_date);

			using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
			{
				return Conn.Query<ObjectFileRecord>(sql_string);
			}
		}


		public int FetchFileRecordsCount(int source_id, string source_type,
									   int harvest_type_id = 1, DateTime? cutoff_date = null)
		{
			string sql_string = "select count(*) ";
			sql_string += source_type.ToLower() == "study" ? "from sf.source_data_studies"
												 : "from sf.source_data_objects";
			sql_string += GetWhereClause(source_id, harvest_type_id, cutoff_date);

			using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
			{
				return Conn.ExecuteScalar<int>(sql_string);
			}
		}


		public IEnumerable<StudyFileRecord> FetchStudyFileRecordsByOffset(int source_id, int offset_num,
									  int amount, int harvest_type_id = 1, DateTime? cutoff_date = null)
		{
			string sql_string = sql_file_select_string;
			sql_string += " from sf.source_data_studies ";
			sql_string += GetWhereClause(source_id, harvest_type_id, cutoff_date);
			sql_string += " offset " + offset_num.ToString() + " limit " + amount.ToString();

			using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
			{
				return Conn.Query<StudyFileRecord>(sql_string);
			}
		}

		public IEnumerable<ObjectFileRecord> FetchObjectFileRecordsByOffset(int source_id, int offset_num,
									 int amount, int harvest_type_id = 1, DateTime? cutoff_date = null)
		{
			string sql_string = sql_file_select_string;
			sql_string += " from sf.source_data_objects ";
			sql_string += GetWhereClause(source_id, harvest_type_id, cutoff_date);
			sql_string += " offset " + offset_num.ToString() + " limit " + amount.ToString();

			using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
			{
				return Conn.Query<ObjectFileRecord>(sql_string);
			}
		}

		private string GetWhereClause(int source_id, int harvest_type_id, DateTime? cutoff_date = null)
		{
			string where_clause = "";
			if (harvest_type_id == 1)
			{
				// Count all files.
				where_clause = " where source_id = " + source_id.ToString();
			}
			else if (harvest_type_id == 2)
			{
				// Count only those files that have been revised (or added) on or since the cutoff date.
				where_clause = " where source_id = " + source_id.ToString() + " and last_revised >= '" + cutoff_date + "'";
			}
			else if (harvest_type_id == 3)
			{
				// For sources with no revision date - Count files unless assumed complete has been set
				// as true (default is null) in which case no further change is expected.
				where_clause = " where source_id = " + source_id.ToString() + " and assume_complete is null";
			}

			where_clause += " and local_path is not null";
			where_clause += " order by local_path";
			return where_clause;
		}

		// get record of interest
		public StudyFileRecord FetchStudyFileRecord(string sd_id, int source_id, string source_type)
		{
			using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
			{
				string sql_string = sql_file_select_string;
				sql_string += " from sf.source_data_studies";
				sql_string += " where sd_id = '" + sd_id + "' and source_id = " + source_id.ToString();
				return Conn.Query<StudyFileRecord>(sql_string).FirstOrDefault();
			}
		}


		public ObjectFileRecord FetchObjectFileRecord(string sd_id, int source_id, string source_type)
		{
			using (NpgsqlConnection Conn = new NpgsqlConnection(connString))
			{
				string sql_string = sql_file_select_string;
				sql_string += " from sf.source_data_objects";
				sql_string += " where sd_id = '" + sd_id + "' and source_id = " + source_id.ToString();
				return Conn.Query<ObjectFileRecord>(sql_string).FirstOrDefault();
			}
		}

		public void UpdateFileRecLastImported(int id, string source_type)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				string sql_string = source_type.ToLower() == "study" ? "update sf.source_data_studies"
														   : "update sf.source_data_objects";
				sql_string += " set last_imported = current_timestamp";
				sql_string += " where id = " + id.ToString();
				conn.Execute(sql_string);
			}
		}

		public int StoreImportEvent(ImportEvent import)
		{
			using (var conn = new NpgsqlConnection(connString))
			{
				return (int)conn.Insert<ImportEvent>(import);
			}
		}


		public bool CheckIfFullHarvest(int source_id)
		{
			string sql_string = @"select type_id from sf.harvest_events
                         where source_id = " + source_id.ToString() + @"
                         and time_ended = (
	                           select max(time_ended) from sf.harvest_events 
                               where source_id = " + source_id.ToString() + @"
                         )";

			using (var conn = new NpgsqlConnection(connString))
			{
				int res = conn.ExecuteScalar<int>(sql_string);
				return (res == 1);
			}
		}

	}
		
}

