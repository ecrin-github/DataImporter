﻿using Dapper.Contrib.Extensions;
using Dapper;
using Npgsql;
using System;
using Microsoft.Extensions.Configuration;
using System.Linq;
using System.Collections.Generic;
using PostgreSQLCopyHelper;
using DataImporter.DBHelpers;

namespace DataImporter.BioLincc
{
	public class BioLinccDataLayer
	{
		private string _mon_connString;
		private string biolincc_connString;
		private string _biolincc_pp_connString;
		private string _ctg_connString;
		private int source_id;

		/// <summary>
		/// Parameterless constructor is used to automatically build
		/// the connection string, using an appsettings.json file that 
		/// has the relevant credentials (but which is not stored in GitHub).
		/// The json file also includes the root folder path, which is
		/// stored in the class's folder_base property.
		/// </summary>
		public BioLinccDataLayer(int _source_id)
		{
			source_id = _source_id;

			IConfigurationRoot settings = new ConfigurationBuilder()
				.SetBasePath(AppContext.BaseDirectory)
				.AddJsonFile("appsettings.json")
				.Build();

			NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
			builder.Host = settings["host"];
			builder.Username = settings["user"];
			builder.Password = settings["password"];

			builder.Database = "mon";
			builder.SearchPath = "sf";
			_mon_connString = builder.ConnectionString;

			builder.Database = "biolincc";
			builder.SearchPath = "sd";
			biolincc_connString = builder.ConnectionString;

			builder.Database = "biolincc";
			builder.SearchPath = "pp";
			_biolincc_pp_connString = builder.ConnectionString;

			builder.Database = "ctg";
			builder.SearchPath = "ad";
			_ctg_connString = builder.ConnectionString; 

			// example appsettings.json file...
			// the only values required are for...
			// {
			//	  "host": "host_name...",
			//	  "user": "user_name...",
			//    "password": "user_password...",
			//	  "folder_base": "C:\\MDR JSON\\Object JSON... "
			// }
		}
			

		public ObjectTypeDetails FetchDocTypeDetails(string doc_name)
		{
			using (var conn = new NpgsqlConnection(_biolincc_pp_connString))
			{
				string sql_string = "Select type_id, type_name from pp.document_types ";
				sql_string += "where resource_name = '" + doc_name + "';";
				return conn.QueryFirstOrDefault<ObjectTypeDetails>(sql_string);
			}
		}


		public SponsorDetails FetchBioLINCCSponsorFromNCT(string nct_id)
		{
			using (var conn = new NpgsqlConnection(_ctg_connString))
			{
				string sql_string = "Select organisation_id as org_id, organisation_name as org_name from ad.study_contributors ";
				sql_string += "where sd_id = '" + nct_id + "' and contrib_type_id = 54;";
				return conn.QueryFirstOrDefault<SponsorDetails>(sql_string);
			}
		}

		public string FetchStudyTitle(string nct_id)
		{
			using (var conn = new NpgsqlConnection(_ctg_connString))
			{
				string sql_string = "Select display_title  from ad.studies ";
				sql_string += "where sd_id = '" + nct_id + "'";
				return conn.QueryFirstOrDefault<string>(sql_string);
			}
		}


		public void StoreStudy(StudyInDB st_db)
		{
			using (var conn = new NpgsqlConnection(biolincc_connString))
			{
				conn.Insert<StudyInDB>(st_db);
			}
		}

		public ulong StoreStudyIdentifiers(PostgreSQLCopyHelper<StudyIdentifier> copyHelper, IEnumerable<StudyIdentifier> entities)
		{
			using (var conn = new NpgsqlConnection(biolincc_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		public ulong StoreStudyTitles(PostgreSQLCopyHelper<StudyTitle> copyHelper, IEnumerable<StudyTitle> entities)
		{
			using (var conn = new NpgsqlConnection(biolincc_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		public ulong StoreStudyRelationships(PostgreSQLCopyHelper<StudyRelationship> copyHelper, IEnumerable<StudyRelationship> entities)
		{
			using (var conn = new NpgsqlConnection(biolincc_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}

		}
		public ulong StoreStudyReferences(PostgreSQLCopyHelper<StudyReference> copyHelper, IEnumerable<StudyReference> entities)
		{
			using (var conn = new NpgsqlConnection(biolincc_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		public ulong StoreDataObjects(PostgreSQLCopyHelper<DataObject> copyHelper, IEnumerable<DataObject> entities)
		{
			using (var conn = new NpgsqlConnection(biolincc_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		public ulong StoreDatasetProperties(PostgreSQLCopyHelper<DataSetProperties> copyHelper, IEnumerable<DataSetProperties> entities)
		{
			using (var conn = new NpgsqlConnection(biolincc_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		public ulong StoreObjectTitles(PostgreSQLCopyHelper<DataObjectTitle> copyHelper,
						IEnumerable<DataObjectTitle> entities)
		{
			using (var conn = new NpgsqlConnection(biolincc_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		public ulong StoreObjectDates(PostgreSQLCopyHelper<DataObjectDate> copyHelper,
						IEnumerable<DataObjectDate> entities)
		{
			using (var conn = new NpgsqlConnection(biolincc_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}

		public ulong StoreObjectInstances(PostgreSQLCopyHelper<DataObjectInstance> copyHelper,
						IEnumerable<DataObjectInstance> entities)
		{
			using (var conn = new NpgsqlConnection(biolincc_connString))
			{
				conn.Open();
				return copyHelper.SaveAll(conn, entities);
			}
		}




	}
}
