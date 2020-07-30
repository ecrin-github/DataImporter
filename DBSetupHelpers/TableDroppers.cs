using Dapper;
using Npgsql;

namespace DataImporter
{
    public class StudyTableDroppers
    {
		string db_conn;

		public StudyTableDroppers(string _db_conn)
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

		public void drop_table_study_features()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.study_features;";
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

		public void drop_table_study_links()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.study_links;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_study_ipd_available()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.study_ipd_available;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}
	}


	public class ObjectTableDroppers
	{
		string db_conn;

		public ObjectTableDroppers(string _db_conn)
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

		public void drop_table_object_languages()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.object_languages;";
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

		public void drop_table_object_corrections()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.object_corrections;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_object_descriptions()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.object_descriptions;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_object_links()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.object_links;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_object_identifiers()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.object_identifiers;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}

		public void drop_table_object_public_types()
		{
			string sql_string = @"DROP TABLE IF EXISTS ad.object_public_types;";
			using (var conn = new NpgsqlConnection(db_conn))
			{
				conn.Execute(sql_string);
			}
		}
	}

}
