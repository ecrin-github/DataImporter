﻿using Dapper.Contrib.Extensions;
using System;

namespace DataImporter
{
    [Table("sf.source_parameters")]
    public class Source : ISource
    {
        public int id { get; set; }
        public string source_type { get; }
        public int? preference_rating { get; set; }
        public string database_name { get; set; }
        public string db_conn { get; set; }
        public int default_harvest_type_id { get; set; }
        public bool requires_file_name { get; set; }
        public bool uses_who_harvest { get; set; }
        public int harvest_chunk { get; }
        public string local_folder { get; set; }
        public bool? local_files_grouped { get; set; }
        public int? grouping_range_by_id { get; set; }
        public string local_file_prefix { get; set; }
        public bool has_study_tables { get; set; }
        public bool has_study_topics { get; set; }
        public bool has_study_features { get; set; }
        public bool has_study_contributors { get; set; }
        public bool has_study_countries { get; set; }
        public bool has_study_references { get; set; }
        public bool has_study_relationships { get; set; }
        public bool has_study_links { get; set; }
        public bool has_study_locations { get; set; }
        public bool has_study_ipd_available { get; set; }
        public bool has_object_datasets { get; set; }
        public bool has_object_dates { get; set; }
        public bool has_object_relationships { get; set; }
        public bool has_object_rights { get; set; }
        public bool has_object_pubmed_set { get; set; }
    }



    [Table("sf.import_events")]
    public class ImportEvent
    {
        [ExplicitKey]
        public int id { get; set; }
        public int source_id { get; set; }
        public DateTime? time_started { get; set; }
        public DateTime? time_ended { get; set; }
        public int? num_new_studies { get; set; }
        public int? num_edited_studies { get; set; }
        public int? num_unchanged_studies { get; set; }
        public int? num_deleted_studies { get; set; }
        public int? num_new_objects { get; set; }
        public int? num_edited_objects { get; set; }
        public int? num_unchanged_objects { get; set; }
        public int? num_deleted_objects { get; set; }
        public string comments { get; set; }

        public ImportEvent(int _id, int _source_id)
        {
            id = _id;
            source_id = _source_id;
            time_started = DateTime.Now;
        }
    }

    [Table("ad.to_agg_imports")]
    public class HistoryRecord
    {
        [ExplicitKey]
        public int id { get; set; }
        public int? num_new_studies { get; set; }
        public int? num_edited_studies { get; set; }
        public int? num_unchanged_studies { get; set; }
        public int? num_deleted_studies { get; set; }
        public int? num_new_objects { get; set; }
        public int? num_edited_objects { get; set; }
        public int? num_unchanged_objects { get; set; }
        public int? num_deleted_objects { get; set; }
        public DateTime? time_created { get; set; }

        public HistoryRecord(ImportEvent imp)
        {
            id = imp.id;
            num_new_studies = imp.num_new_studies;
            num_edited_studies = imp.num_edited_studies;
            num_unchanged_studies = imp.num_unchanged_studies;
            num_deleted_studies = imp.num_deleted_studies;
            num_new_objects = imp.num_new_objects;
            num_edited_objects = imp.num_edited_objects;
            num_unchanged_objects = imp.num_unchanged_objects;
            num_deleted_objects = imp.num_deleted_objects;
            time_created = DateTime.Now;
        }
    }

    [Table("sf.source_data_studies")]
    public class StudyFileRecord
    {
        public int id { get; set; }
        public int source_id { get; set; }
        public string sd_id { get; set; }
        public string remote_url { get; set; }
        public DateTime? last_revised { get; set; }
        public bool? assume_complete { get; set; }
        public int download_status { get; set; }
        public string local_path { get; set; }
        public int last_saf_id { get; set; }
        public DateTime? last_downloaded { get; set; }
        public int last_harvest_id { get; set; }
        public DateTime? last_harvested { get; set; }
        public int last_import_id { get; set; }
        public DateTime? last_imported { get; set; }

        // constructor when a revision data can be expected (not always there)
        public StudyFileRecord(int _source_id, string _sd_id, string _remote_url, int _last_saf_id,
                                              DateTime? _last_revised, string _local_path)
        {
            source_id = _source_id;
            sd_id = _sd_id;
            remote_url = _remote_url;
            last_saf_id = _last_saf_id;
            last_revised = _last_revised;
            download_status = 2;
            last_downloaded = DateTime.Now;
            local_path = _local_path;
        }

        // constructor when an 'assumed complete' judgement can be expected (not always there)
        public StudyFileRecord(int _source_id, string _sd_id, string _remote_url, int _last_saf_id,
                                              bool? _assume_complete, string _local_path)
        {
            source_id = _source_id;
            sd_id = _sd_id;
            remote_url = _remote_url;
            last_saf_id = _last_saf_id;
            assume_complete = _assume_complete;
            download_status = 2;
            last_downloaded = DateTime.Now;
            local_path = _local_path;
        }


        public StudyFileRecord()
        { }

    }


    [Table("sf.source_data_objects")]
    public class ObjectFileRecord
    {
        public int id { get; set; }
        public int source_id { get; set; }
        public string sd_id { get; set; }
        public string remote_url { get; set; }
        public DateTime? last_revised { get; set; }
        public bool? assume_complete { get; set; }
        public int download_status { get; set; }
        public string local_path { get; set; }
        public int last_saf_id { get; set; }
        public DateTime? last_downloaded { get; set; }
        public int last_harvest_id { get; set; }
        public DateTime? last_harvested { get; set; }
        public int last_import_id { get; set; }
        public DateTime? last_imported { get; set; }

        // constructor when a revision data can be expected (not always there)
        public ObjectFileRecord(int _source_id, string _sd_id, string _remote_url, int _last_saf_id,
                                              DateTime? _last_revised, string _local_path)
        {
            source_id = _source_id;
            sd_id = _sd_id;
            remote_url = _remote_url;
            last_saf_id = _last_saf_id;
            last_revised = _last_revised;
            download_status = 2;
            last_downloaded = DateTime.Now;
            local_path = _local_path;
        }

        // constructor when an 'assumed complete' judgement can be expected (not always there)
        public ObjectFileRecord(int _source_id, string _sd_id, string _remote_url, int _last_saf_id,
                                              bool? _assume_complete, string _local_path)
        {
            source_id = _source_id;
            sd_id = _sd_id;
            remote_url = _remote_url;
            last_saf_id = _last_saf_id;
            assume_complete = _assume_complete;
            download_status = 2;
            last_downloaded = DateTime.Now;
            local_path = _local_path;
        }

        public ObjectFileRecord()
        { }

    }

    public class att_stat
    {
        public int status { get; set; }
        public int num { get; set; }
    }


    public class hash_stat
    {
        public int hash_type_id { get; set; }
        public string hash_type { get; set; }
        public int num { get; set; }
    }

}
