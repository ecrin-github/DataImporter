using System;
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

		public void SetupTempTables()
		{
			repo.SetupTempTables();
		}

		public void AddNewStudies()
		{
			repo.TransferStudies();
			repo.TransferDataObjects();
		}

		public void LoopThroughMatchedStudies()
		{
			//
		}

		public void DeleteTempTables()
		{
			repo.DeleteTempTables();
		}
	}

}
