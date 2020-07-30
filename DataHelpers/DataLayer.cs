using Dapper.Contrib.Extensions;
using Dapper;
using Npgsql;
using System;
using Microsoft.Extensions.Configuration;
using System.Linq;
using System.Collections.Generic;
using DataImporter.DBHelpers;

namespace DataImporter
{
	public class DataLayer
	{
		private string connString;

		/// <summary>
		/// /// Parameterless constructor is used to automatically build
		/// the connection string, using an appsettings.json file that 
		/// has the relevant credentials (but which is not stored in GitHub).
		/// The json file also includes the root folder path, which is
		/// stored in the class's folder_base property.
		/// </summary>
		public DataLayer(Source source)
		{
			IConfigurationRoot settings = new ConfigurationBuilder()
				.SetBasePath(AppContext.BaseDirectory)
				.AddJsonFile("appsettings.json")
				.Build();

			NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
			builder.Host = settings["host"];
			builder.Username = settings["user"];
			builder.Password = settings["password"];
			builder.Database = source.database_name;
			connString = builder.ConnectionString;

			/*
			builder.Database = "mon";
			builder.SearchPath = "sf";
			_connString = builder.ConnectionString;

			builder.Database = "biolincc";
			builder.SearchPath = "pp";
			_biolincc_pp_connString = builder.ConnectionString;
	        
			builder.Database = "isrctn";
			builder.SearchPath = "ad";
			_isrctn_connString = builder.ConnectionString; */

		}

		public string ConnString => connString;

	}
}
