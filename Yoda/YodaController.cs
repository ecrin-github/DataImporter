﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml.Serialization;
using DataImporter.DBHelpers;

namespace DataImporter.Yoda
{
    
	class YodaController
	{
		DataLayer common_repo; 
		YodaDataLayer repo;
		YodaProcessor processor;
		int source_id;

		public YodaController(DataLayer _common_repo, int _source_id)
		{
			source_id = _source_id;
			repo = new YodaDataLayer(source_id);
			processor = new YodaProcessor();
			common_repo = _common_repo;
		}

		public void DropADTables()
		{
			repo.DeleteADStudyTables();
			repo.DeleteADObjectTables();
		}

		public void EstablishNewADTables()
		{
			repo.BuildNewADStudyTables();
			repo.BuildNewADObjectTables();
		}

		public void LoopThroughRecords()
		{
			// at the moment, for Yoda and BioLincc, harvest_type_id always 1
			// i.e. examine all files and transfer them to the sd tables 

			// Get the folder base from the appsettings file
			// and construct a list of the files 
			// N.B. (only one folder for all files) 

			IEnumerable<FileRecord> file_list = common_repo.FetchStudyFileRecords(source_id);
			int n = 0; string filePath = "";
			foreach (FileRecord rec in file_list)
			{
				n++;
				// for testing...
				//if (n == 50) break;

				filePath = rec.local_path;
				if (File.Exists(filePath))
				{
					string inputString = "";
					using (var streamReader = new StreamReader(filePath, System.Text.Encoding.UTF8))
					{
						inputString += streamReader.ReadToEnd();
					}

					XmlSerializer serializer = new XmlSerializer(typeof(YodaRecord));
					StringReader rdr = new StringReader(inputString);
					YodaRecord studyRegEntry = (YodaRecord)serializer.Deserialize(rdr);

					// break up the file into relevant data classes
					processor.ProcessData(repo);

					// store the data in the database			
					processor.StoreData(repo);  

					// update file record with last processed datetime
					common_repo.UpdateStudyFileRecLastProcessed(rec.id);

				}

				if (n % 10 == 0) Console.WriteLine(n.ToString());
			}
		}

		public void InsertHashes()
		{
			//
		}
	}

}
