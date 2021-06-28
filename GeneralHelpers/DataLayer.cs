using Dapper.Contrib.Extensions;
using Microsoft.Extensions.Configuration;
using Npgsql;
using System;


namespace DataImporter
{
    public class DataLayer
    {
        private Source source;
        private string connString;
        private string user_name;
        private string password;

        public DataLayer(int source_id, bool is_test)
        {
            IConfigurationRoot settings = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json")
                .Build();

            NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
            builder.Host = settings["host"];
            builder.Username = settings["user"];
            builder.Password = settings["password"];

            user_name = settings["user"];
            password = settings["password"];

            // Initial call into mon database to get source parameters

            builder.Database = "mon";
            string mon_connString = builder.ConnectionString;

            using (NpgsqlConnection Conn = new NpgsqlConnection(mon_connString))
            {
                source = Conn.Get<Source>(source_id);
                if (source != null)
                {
                    if (!is_test)
                    {
                        builder.Database = source.database_name;
                    }
                    else
                    {
                        builder.Database = "test";
                    }
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

        public string User_Name => user_name;
        public string Password => password;

    }

}
