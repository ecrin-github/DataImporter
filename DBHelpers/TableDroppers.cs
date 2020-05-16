using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataImporter.DBHelpers
{
    public class StudyADTableDroppers
    {
		string db_conn;

		public StudyADTableDroppers(string _db_conn)
		{
			db_conn = _db_conn;
		}
		
		public void drop_table_studies()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.studies;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_study_identifiers()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.study_identifiers;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_study_titles()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.study_titles;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_study_contributors()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.study_contributors;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_study_topics()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.study_topics;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_study_relationships()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.study_relationships;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_study_references()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.study_references;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_study_hashes()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.study_hashes;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}
	}

	public class ObjectADTableDroppers
	{
		string db_conn;

		public ObjectADTableDroppers(string _db_conn)
		{
			db_conn = _db_conn;
		}
		
		public void drop_table_data_objects()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.data_objects;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_dataset_properties()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.dataset_properties;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_object_dates()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.object_dates;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_object_instances()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.object_instances;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_object_titles()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.object_titles;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_object_contributors()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.object_contributors;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_object_topics()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.object_topics;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_object_hashes()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.object_hashes;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}
	}

}
