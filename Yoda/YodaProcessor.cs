﻿using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Net;
using System.Web;
using System.Runtime.InteropServices.ComTypes;
using DataImporter;
using DataImporter.DBHelpers;


namespace DataImporter.Yoda
{
	public class YodaProcessor
	{
		HelperFunctions hp;


		public YodaProcessor()
		{
			hp = new HelperFunctions();
		}


		public Study ProcessData(YodaDataLayer repo, YodaRecord st, DateTime? download_datetime)
		{
			Study s = new Study();

			// get date retrieved in object fetch
			// transfer to study and data object records

			List<StudyIdentifier> study_identifiers = new List<StudyIdentifier>();
			List<StudyTitle> study_titles = new List<StudyTitle>();
			List<StudyReference> study_references = new List<StudyReference>();
			List<StudyContributor> study_contributors = new List<StudyContributor>();
			List<StudyTopic> study_topics = new List<StudyTopic>();


			List<DataObject> data_objects = new List<DataObject>();
			List<DataSetProperties> dataset_properties = new List<DataSetProperties>();
			List<DataObjectTitle> data_object_titles = new List<DataObjectTitle>();
			List<DataObjectDate> data_object_dates = new List<DataObjectDate>();
			List<DataObjectInstance> data_object_instances = new List<DataObjectInstance>();

			string access_details = "The YODA Project will require that requestors provide basic information about the Principal Investigator, Key Personnel, and the ";
			access_details += "project Research Proposal, including a scientific abstract and research methods.The YODA Project will review proposals to ensure that: ";
			access_details += "1) the scientific purpose is clearly described; 2) the data requested will be used to enhance scientific and/or medical knowledge; and ";
			access_details += "3) the proposed research can be reasonably addressed using the requested data.";


			// transfer features of main study object
			// In most cases study will have already been registered in CGT.

			s.sd_id = st.sd_id;
			s.datetime_of_data_fetch = download_datetime;

			if (st.display_title.Contains("<"))
			{
				s.display_title = hp.replace_tags(st.display_title);
				s.display_title = hp.strip_tags(s.display_title);
			}
			else
			{
				s.display_title = st.display_title;
			}

			/*
			// No brief description available here 
			if (st.brief_description.Contains("<"))
			{
				s.brief_description = hp.replace_tags(st.brief_description);
				s.bd_contains_html = hp.check_for_tags(s.brief_description);
			}
			else
			{
				s.brief_description = st.brief_description;
			}

			// No study specific data sharing statement available
			if (st.data_sharing_statement.Contains("<"))
			{
			    s.data_sharing_statement = strip_tags(st.data_sharing_statement);
			    s.dss_contains_html = check_for_tags(s.data_sharing_statement);
			}
			*/

			s.study_status_id = 21;
			s.study_status = "Completed";  // assumption for entry onto web site

			// previously obtained from the ctg or isrctn entry
			s.study_type_id = st.type_id;
			if (st.type_id == 11)
			{
			    s.study_type = "Interventional";
			}
			else if (st.type_id == 12)
			{
				s.study_type = "Observational";
			}

			// study type only really relevant for non registered studies (others will  
			// have type identified in registered study entry
			

     		if (Int32.TryParse(st.enrolment, out int enrolment_number))
			{
                s.study_enrolment = enrolment_number;
			}

			string percent_female = st.percent_female;
			if (!string.IsNullOrEmpty(percent_female) && percent_female != "N/A")
			{
				if (percent_female.EndsWith("%"))
				{
					percent_female = percent_female.Substring(0, percent_female.Length - 1);
				}

				if (Single.TryParse(st.enrolment, out float female_percentage))
				{
					if (female_percentage == 0)
					{
						s.study_gender_elig_id = 910;
						s.study_gender_elig = "Male";
					}
					else if (female_percentage == 100)
					{
						s.study_gender_elig_id = 905;
						s.study_gender_elig = "Female";
					}
					else
					{
						s.study_gender_elig_id = 900;
						s.study_gender_elig = "All";
					}
				}
			}
			else
			{
				s.study_gender_elig_id = 915;
				s.study_gender_elig = "Not provided";
			}

			// transfer title data
			if (st.study_titles.Count > 0)
			{
				foreach(Title t in st.study_titles)
				{
					study_titles.Add(new StudyTitle(s.sd_id, t.title_text, t.title_type_id, t.title_type, t.is_default, t.comments));
				}
			}

			// transfer identifier data
			// Normally a protocol id will be the only addition (may be a duplicate of one already in the system)
			if (st.study_identifiers.Count > 0)
			{
				foreach(Identifier i in st.study_identifiers)
				{
					study_identifiers.Add(new StudyIdentifier(s.sd_id, i.identifier_value, i.identifier_type_id, i.identifier_type,
													  i.identifier_org_id, i.identifier_org));
				}
			}

			// study contributors
			// only sponsor knowm, and only relevant for non registered studies (others will  
			// have type identified in registered study entry).
			int? sponsor_org_id; string sponsor_org;
			if (!string.IsNullOrEmpty(st.sponsor))
			{
				sponsor_org_id = st.sponsor_id;
				sponsor_org = st.sponsor;
			}
			else
			{
				sponsor_org_id = null; 
				sponsor_org = "No organisation name provided in source data";
			}

			if (st.is_yoda_only)
			{
				study_contributors.Add(new StudyContributor(s.sd_id, 54, "Study Sponsor", sponsor_org_id, sponsor_org, null, null));
			}

			// study topics
			// create a list for all studies as maty be merged with pre-existing entries for the same study
			// (if not already present)

			if (!string.IsNullOrEmpty(st.compound_generic_name))
			{
				study_topics.Add(new StudyTopic(s.sd_id, 12, "chemical / agent", st.compound_generic_name, null, "Yoda"));
			}
			if (!string.IsNullOrEmpty(st.compound_product_name))
			{
				study_topics.Add(new StudyTopic(s.sd_id, 12, "chemical / agent", st.compound_product_name, null, "Yoda"));
			}
			if (!string.IsNullOrEmpty(st.conditions_studied))
			{
				study_topics.Add(new StudyTopic(s.sd_id, 13, "condition", st.compound_product_name, null, "Yoda"));
			}

			// create study references (pmids)
			if (st.study_references.Count > 0)
			{
				foreach (Reference r in st.study_references)
				{
					// normally only 1 if there is one there at all 
					study_references.Add(new StudyReference(s.sd_id, r.pmid, st.primary_citation_link, "", ""));
				}
			}

		
			// data objects...

			string name_base = string.IsNullOrEmpty(st.public_title) ? s.display_title : st.public_title;
			int do_id = 1;
			
			// do the yoda web page itself first...
			string object_display_title = name_base + " :: " + "Yoda web page";
			data_objects.Add(new DataObject(st.sd_id, do_id, object_display_title, null, 23, "Text", 38, "Study Overview",
			                  101901, "Yoda", 12, download_datetime));
			data_object_titles.Add(new DataObjectTitle(st.sd_id, do_id, object_display_title, 22,
							"Study short name :: object type", true));
			data_object_instances.Add(new DataObjectInstance(st.sd_id, do_id, 101901, "Yoda",
								st.remote_url, true, 35, "Web text"));

			// then for each supp doc...
			if (st.supp_docs.Count > 0)
			{
				foreach (SuppDoc sd in st.supp_docs)
				{
					do_id++;
					// get object_type
					int object_class_id = 0; string object_class = "";
					int object_type_id = 0; string object_type = "";

					switch (sd.doc_name)
					{
						case "Collected Datasets":
							{
								object_type_id = 80;
								object_type = "Individual Participant Data";
								object_class_id = 14; object_class = "Datasets";
								break;
							}
						case "Data Definition Specification":
							{
								object_type_id = 31;
								object_type = "Data Dictionary";
								object_class_id = 23; object_class = "Text";
								break;
							}
						case "Analysis Datasets":
							{
								object_type_id = 51;
								object_type = "IPD final analysis datasets (full study population)";
								object_class_id = 14; object_class = "Datasets";
								break;
							}
						case "CSR Summary":
							{
								object_type_id = 79;
								object_type = "CSR Summary";
								object_class_id = 23; object_class = "Text";
								break;
							}
						case "Annotated Case Report Form":
							{
								object_type_id = 30;
								object_type = "Annotated Data Collection Forms";
								object_class_id = 23; object_class = "Text";
								break;
							}
						case "Statistical Analysis Plan":
							{
								object_type_id = 22;
								object_type = "Statistical analysis plan";
								object_class_id = 23; object_class = "Text";
								break;
							}
						case "Protocol with Amendments":
							{
								object_type_id = 11;
								object_type = "Study Protocol";
								object_class_id = 23; object_class = "Text";
								break;
							}
						case "Clinical Study Report":
							{
								object_type_id = 26;
								object_type = "Clinical Study Report";
								object_class_id = 23; object_class = "Text";
								break;
							}
					}

					object_display_title = name_base + " :: " + object_type;
					if (sd.comment == "Available now")
					{
						data_objects.Add(new DataObject(st.sd_id, do_id, object_display_title, null, object_class_id, object_class, object_type_id, object_type,
										101901, "Yoda", 11, download_datetime));
						data_object_titles.Add(new DataObjectTitle(st.sd_id, do_id, object_display_title, 22,"Study short name :: object type", true));

						// create instance as resource exists
						// get file type from link if possible
						int resource_type_id = 0; string resource_type = "";
						if (sd.url.ToLower().EndsWith(".pdf"))
						{
							resource_type_id = 11;
							resource_type = "PDF";
						}
						else if (sd.url.ToLower().EndsWith(".xls"))
						{
							resource_type_id = 18;
							resource_type = "Excel Spreadsheet(s)";
						}
						else
						{
							resource_type_id = 0;
							resource_type = "Not yet known";
						}
						data_object_instances.Add(new DataObjectInstance(st.sd_id, do_id, 101901, "Yoda", sd.url, true, resource_type_id, resource_type));
					}
					else
					{
						data_objects.Add(new DataObject(st.sd_id, do_id, object_display_title, null, object_class_id, object_class, object_type_id, object_type,
										101901, "Yoda", 17, "Case by case download", access_details,
										"https://yoda.yale.edu/how-request-data", new DateTime(2020, 05, 15), download_datetime));
						data_object_titles.Add(new DataObjectTitle(st.sd_id, do_id, object_display_title, 22, "Study short name :: object type", true));
					}

					// for datasets also add dataset properties - even if they are largely unknown
					if (object_type_id == 80)
					{
						dataset_properties.Add(new DataSetProperties(s.sd_id, do_id, 0, "Not yet known", "", 
							                      2 , "De-identified", 
												  "Yoda states that '...researchers will be granted access to participant-level study data that are devoid of personally identifiable information; current best guidelines for de-identification of data will be used.'", 
												  0, "Not yet known", ""));
					}
				}
			}

			// add in the study properties
			s.identifiers = study_identifiers;
			s.titles = study_titles;
			s.references = study_references;
			s.contributors = study_contributors;
			s.topics = study_topics;

			s.data_objects = data_objects;
			s.dataset_properties = dataset_properties;
			s.object_titles = data_object_titles;
			s.object_instances = data_object_instances;

			return s;
		}


		public void StoreData(YodaDataLayer repo, Study s)
		{
			// store study
			StudyInDB st = new StudyInDB(s);
			repo.StoreStudy(st);

			// store study attributes
			if (s.identifiers.Count > 0)
			{
				repo.StoreStudyIdentifiers(CopyHelpers.study_ids_helper,
										  s.identifiers);
			}

			if (s.titles.Count > 0)
			{
				repo.StoreStudyTitles(CopyHelpers.study_titles_helper,
										  s.titles);
			}

			if (s.references.Count > 0)
			{
				repo.StoreStudyReferences(CopyHelpers.study_references_helper,
										  s.references);
			}

			if (s.contributors.Count > 0)
			{
				repo.StoreStudyContributors(CopyHelpers.study_contributors_helper, 
										  s.contributors);
			}

			if (s.topics.Count > 0)
			{
				repo.StoreStudyTopics(CopyHelpers.study_topics_helper,
										  s.topics);
			}


			// store data objects and dataset properties
			if (s.data_objects.Count > 0)
			{
				repo.StoreDataObjects(CopyHelpers.data_objects_helper,
										 s.data_objects);
			}

			if (s.dataset_properties.Count > 0)
			{
				repo.StoreDatasetProperties(CopyHelpers.dataset_properties_helper,
										 s.dataset_properties);
			}

			// store data object attributes
			if (s.object_instances.Count > 0)
			{
				repo.StoreObjectInstances(CopyHelpers.object_instances_helper,
										 s.object_instances);
			}

			if (s.object_titles.Count > 0)
			{
				repo.StoreObjectTitles(CopyHelpers.object_titles_helper,
										 s.object_titles);
			}
		}
	}
}


