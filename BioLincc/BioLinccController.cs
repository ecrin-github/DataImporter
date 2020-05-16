using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml.Serialization;
using DataImporter.DBHelpers;

namespace DataImporter.BioLincc
{
    public class BioLinccController
	{
		DataLayer common_repo;
		BioLinccDataLayer repo;
		BioLinccProcessor processor;
		int source_id;

		public BioLinccController(DataLayer _common_repo, int _source_id)
		{
			source_id = _source_id;
			repo = new BioLinccDataLayer(source_id);
			processor = new BioLinccProcessor();
			common_repo = _common_repo;
		}

		public void DropADTables()
		{
			repo.DeleteADStudyTables();
			repo.DeleteADObjectTables();
		}

		public void EstablishNewADTables()
		{
			// only built if athey do not already exist
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
