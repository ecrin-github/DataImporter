using Dapper.Contrib.Extensions;
using Dapper;
using Npgsql;
using System;
using Microsoft.Extensions.Configuration;


namespace DataImporter
{
	public class DataLayer
	{
        private Source source;
		private string connString;

		public DataLayer(int source_id)
		{
			IConfigurationRoot settings = new ConfigurationBuilder()
				.SetBasePath(AppContext.BaseDirectory)
				.AddJsonFile("appsettings.json")
				.Build();

			NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
			builder.Host = settings["host"];
			builder.Username = settings["user"];
			builder.Password = settings["password"];

			// Initial call into mon database to ghet source parameters

			builder.Database = "mon";
			string mon_connString = builder.ConnectionString;

			using (NpgsqlConnection Conn = new NpgsqlConnection(mon_connString))
			{
				source = Conn.Get<Source>(source_id);
				if (source != null)
                {
                    builder.Database = source.database_name;
			        connString = builder.ConnectionString;
                }
				else
                {
					connString = "";
				}
			}

		}

		public string ConnString => connString;
		public Source Source => source;

	}


	[Table("sf.source_parameters")]
	public class Source
	{
		public int id { get; set; }
		public int? preference_rating { get; set; }
		public string database_name { get; set; }
		public int default_harvest_type_id { get; set; }
		public bool requires_file_name { get; set; }
		public bool uses_who_harvest { get; set; }
		public string local_folder { get; set; }
		public bool? local_files_grouped { get; set; }
		public int? grouping_range_by_id { get; set; }
		public string local_file_prefix { get; set; }
		public bool has_study_tables { get; set; }
		public bool has_study_topics { get; set; }
		public bool has_study_features { get; set; }
		public bool has_study_contributors { get; set; }
		public bool has_study_references { get; set; }
		public bool has_study_relationships { get; set; }
		public bool has_study_links { get; set; }
		public bool has_study_ipd_available { get; set; }
		public bool has_dataset_properties { get; set; }
		public bool uses_language_default { get; set; }
		public bool has_object_languages { get; set; }
		public bool has_object_dates { get; set; }
		public bool has_object_pubmed_set { get; set; }
	}

}
