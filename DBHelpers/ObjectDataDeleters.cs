using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataImporter
{
	class DataObjectDataDeleter
	{
		string connstring;

		public DataObjectDataDeleter(string _connstring)
		{
            connstring = _connstring;
		}

		public void DeleteDataObjects()
		{
            // if the record hash for the data object has changed, then 
            // the data in the data objects records should be changed

            string sql_string = @"with t as (
			      select * from ad.temp_data_objects
			      where status = 4)
			  delete from ad.data_objects
              using t
			  where sd_oid = t.sd_oid;";

            using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}


        public void DeleteDataSetProperties()
		{
            string sql_string = @"with t as (
			      select * from ad.temp_data_objects
			      where status = 4)
			  delete from ad.dataset_properties
              using t
			  where sd_oid = t.sd_oid;";

			using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}

		public void DeleteObjectInstances()
		{
            string sql_string = @"with t as (
			      select * from ad.temp_data_objects
			      where status = 4)
			  delete from ad.object_instances
              using t
			  where sd_oid = t.sd_oid;";

            using (var conn = new NpgsqlConnection(connstring))
            {
                conn.Execute(sql_string);
            }

        }

		public void DeleteObjectTitles()
		{
            string sql_string = @"with t as (
			      select * from ad.temp_data_objects
			      where status = 4)
			  delete from ad.object_titles
              using t
			  where sd_oid = t.sd_oid;";

            using (var conn = new NpgsqlConnection(connstring))
            {
                conn.Execute(sql_string);
            }

        }


		public void DeleteObjectLanguages()
		{
            string sql_string = @"with t as (
			      select * from ad.temp_data_objects
			      where status = 4)
			  delete from ad.object_languages
              using t
			  where sd_oid = t.sd_oid;";

            using (var conn = new NpgsqlConnection(connstring))
            {
                conn.Execute(sql_string);
            }

        }

		public void DeleteObjectDates()
		{
            string sql_string = @"with t as (
			      select * from ad.temp_data_objects
			      where status = 4)
			  delete from ad.object_dates
              using t
			  where sd_oid = t.sd_oid;";

            using (var conn = new NpgsqlConnection(connstring))
            {
                conn.Execute(sql_string);
            }

        }

        public void DeleteObjectContributors()
		{
            string sql_string = @"with t as (
			      select * from ad.temp_data_objects
			      where status = 4)
			  delete from ad.object_contributors
              using t
			  where sd_oid = t.sd_oid;";

            using (var conn = new NpgsqlConnection(connstring))
            {
                conn.Execute(sql_string);
            }
        }

		public void DeleteObjectTopics()
		{
            string sql_string = @"with t as (
			      select * from ad.temp_data_objects
			      where status = 4)
			  delete from ad.object_topics
              using t
			  where sd_oid = t.sd_oid;";

            using (var conn = new NpgsqlConnection(connstring))
            {
                conn.Execute(sql_string);
            }
        }


        public void DeleteObjectCorrections()
		{
            string sql_string = @"with t as (
			      select * from ad.temp_data_objects
			      where status = 4)
			  delete from ad.object_corrections
              using t
			  where sd_oid = t.sd_oid;";

            using (var conn = new NpgsqlConnection(connstring))
            {
                conn.Execute(sql_string);
            }
        }


        public void DeleteObjectDescriptions()
		{
            string sql_string = @"with t as (
			      select * from ad.temp_data_objects
			      where status = 4)
			  delete from ad.object_descriptions
              using t
			  where sd_oid = t.sd_oid;";

            using (var conn = new NpgsqlConnection(connstring))
            {
                conn.Execute(sql_string);
            }
        }


		public void DeleteObjectIdentifiers()
		{
            string sql_string = @"with t as (
			      select * from ad.temp_data_objects
			      where status = 4)
			  delete from ad.object_identifiers
              using t
			  where sd_oid = t.sd_oid;";

            using (var conn = new NpgsqlConnection(connstring))
            {
                conn.Execute(sql_string);
            }
        }


        public void DeleteObjectLinks()
		{
            string sql_string = @"with t as (
			      select * from ad.temp_data_objects
			      where status = 4)
			  delete from ad.object_links
              using t
			  where sd_oid = t.sd_oid;";

            using (var conn = new NpgsqlConnection(connstring))
            {
                conn.Execute(sql_string);
            }
        }


        public void DeleteObjectPublic_types()
		{
            string sql_string = @"with t as (
			      select * from ad.temp_data_objects
			      where status = 4)
			  delete from ad.object_public_types
              using t
			  where sd_oid = t.sd_oid;";

            using (var conn = new NpgsqlConnection(connstring))
            {
                conn.Execute(sql_string);
            }
        }


        public void DeleteObjectHashes()
		{

            // Need to ensure that the hashes themselves are all up to date
            // Change the ones that have been changed in sd
            string sql_string = @"with t as (
			      select * from ad.temp_data_objects
			      where status = 4)
			  delete from ad.object_hashes
              using t
			  where sd_oid = t.sd_oid;";

            using (var conn = new NpgsqlConnection(connstring))
			{
				conn.Execute(sql_string);
			}
		}

	}

}
