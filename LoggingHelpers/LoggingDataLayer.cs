using Dapper;
using Dapper.Contrib.Extensions;
using Microsoft.Extensions.Configuration;
using Npgsql;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DataImporter
{
    public class LoggingDataLayer
    {
        private string connString;
        private Source source;
        private string host;
        private string user;
        private string password;
        private string sql_file_select_string;
        private string logfile_startofpath;
        private string logfile_path;
        private StreamWriter sw;

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
            host = settings["host"];
            user = settings["user"];
            password = settings["password"];

            builder.Host = host;
            builder.Username = user;
            builder.Password = password;

            builder.Database = "mon";
            connString = builder.ConnectionString;

            logfile_startofpath = settings["logfilepath"];

            sql_file_select_string = "select id, source_id, sd_id, remote_url, last_revised, ";
            sql_file_select_string += " assume_complete, download_status, local_path, last_saf_id, last_downloaded, ";
            sql_file_select_string += " last_harvest_id, last_harvested, last_import_id, last_imported ";
        }

        public Source SourceParameters => source;


        public void OpenLogFile(string database_name)
        {
            string dt_string = DateTime.Now.ToString("s", System.Globalization.CultureInfo.InvariantCulture)
                              .Replace("-", "").Replace(":", "").Replace("T", " ");
            logfile_path = logfile_startofpath + "IM " + database_name + " " + dt_string + ".log";
            sw = new StreamWriter(logfile_path, true, System.Text.Encoding.UTF8);
        }

        public void LogLine(string message, string identifier = "")
        {
            string dt_string = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
            string feedback = dt_string + message + identifier;
            Transmit(feedback);
        }

        public void LogHeader(string message)
        {
            string dt_string = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
            string header = dt_string + "**** " + message + " ****";
            Transmit("");
            Transmit(header);
        }

        public void LogError(string message)
        {
            string dt_string = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
            string error_message = dt_string + "***ERROR*** " + message;
            Transmit("");
            Transmit("+++++++++++++++++++++++++++++++++++++++");
            Transmit(error_message);
            Transmit("+++++++++++++++++++++++++++++++++++++++");
            Transmit("");
        }

        public void LogDiffs(string db_conn, Source s)
        {
            // Gets and logs record count for each table in the sd schema of the database
            
            LogHeader("SD - AD Differences");
            if (s.has_study_tables)
            {
                LogLine(GetTableRecordCount(db_conn, "to_ad_study_recs"));
                LogLine(GetEntityRecDiffs(db_conn, "study"));
                GetStudyStats(db_conn, "recs");
                LogLine(GetTableRecordCount(db_conn, "to_ad_study_atts"));
                GetStudyStats(db_conn, "atts");
            }
            LogLine(GetTableRecordCount(db_conn, "to_ad_object_recs"));
            LogLine(GetEntityRecDiffs(db_conn, "object"));
            LogLine(GetDatasetRecDiffs(db_conn));
            GetObjectStats(db_conn, "recs");
            LogLine(GetTableRecordCount(db_conn, "to_ad_object_atts"));
            GetObjectStats(db_conn, "atts");
        }

        public void CloseLog()
        {
            LogHeader("Closing Log");
            sw.Flush();
            sw.Close();
        }


        private string GetTableRecordCount(string db_conn, string table_name)
        {
            string sql_string = "select count(*) from sd." + table_name;

            using (NpgsqlConnection conn = new NpgsqlConnection(db_conn))
            {
                int res = conn.ExecuteScalar<int>(sql_string);
                return res.ToString() + " records found in sd." + table_name;
            }
        }


        private void GetStudyStats(string db_conn, string table_type)
        {
            string sql_string = "select status, count(sd_sid) as num from sd.to_ad_study_" + table_type;
            sql_string += " group by status order by status;";
            GetAndWriteStats(db_conn, sql_string);
        }

        private void GetObjectStats(string db_conn, string table_type)
        {
            string sql_string = "select status, count(sd_oid) as num from sd.to_ad_object_" + table_type;
            sql_string += " group by status order by status;";
            GetAndWriteStats(db_conn, sql_string);
        }

        private void GetAndWriteStats(string db_conn, string sql_string)
        {
            IEnumerable<att_stat> status_stats;
            using (NpgsqlConnection conn = new NpgsqlConnection(db_conn))
            {
                status_stats = conn.Query<att_stat>(sql_string);
            }
            if (status_stats.Count() > 0)
            {
                foreach (att_stat hs in status_stats)
                {
                    LogLine("Status " + hs.status.ToString() + ": " + hs.num.ToString());
                }
            }
            LogLine("");
        }

        private string GetEntityRecDiffs(string db_conn, string entity_type)
        {
            string table_name = (entity_type == "study") ? "to_ad_study_recs" : "to_ad_object_recs";
            string sql_string = "select count(*) from sd." + table_name + 
                                " where " + entity_type + "_rec_status = 2;";

            using (NpgsqlConnection conn = new NpgsqlConnection(db_conn))
            {
                int res = conn.ExecuteScalar<int>(sql_string);
                return res.ToString() + " records found with edits to the " + entity_type + " record itself;";
            }
        }

        private string GetDatasetRecDiffs(string db_conn)
        {
            string sql_string = @"select count(*) from sd.to_ad_object_recs
                                 where object_dataset_status = 4;";
            using (NpgsqlConnection conn = new NpgsqlConnection(db_conn))
            {
                int res = conn.ExecuteScalar<int>(sql_string);
                return res.ToString() + " records found with edits to the dataset data;";
            }
        }

        private void Transmit(string message)
        {
            sw.WriteLine(message);
            Console.WriteLine(message);
        }

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
                return (last_id == 0) ? 10001 : last_id + 1;
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
            import.time_ended = DateTime.Now;
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

