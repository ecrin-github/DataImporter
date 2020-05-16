using System;
using DataImporter.Yoda;
using DataImporter.BioLincc;
using static System.Console;
using System.Security.Cryptography.X509Certificates;

namespace DataImporter
{
	class Program
	{
		static void Main(string[] args)
		{
			bool create_tables = false;
			int source_id = 0;

			if (args.Length == 0 && args.Length > 2)
			{
				WriteLine("sorry - one or two parameters are necessary");
				WriteLine("The first a string to indicate the source");
				WriteLine("The second either 0 or 1 to indicate whether tables need to be recreated");
			}
			else
			{
				string source = args[0];
				switch (source.ToLower()[0])
				{
					case 'b':
						{
							source_id = 100900; break; // biolincc
						}
					case 'y':
						{
							source_id = 100901; break; // yoda
						}
				}

				if (source_id == 0)
				{
					WriteLine("sorry - I don't recognise that source argument");
				}

				if (args.Length == 2)
				{
					string table_create = args[1];
					// should be '0' or '1'
					// to indicate create new ad tables use 1 
					// default is 0, leave files as they are!
					if (table_create == "1")
					{
						create_tables = true; // recreate tables
					}
				}
			}


			// proceed if both required parameters are valid
			if (source_id > 0)
			{
				DataLayer repo = new DataLayer();

				switch (source_id)
				{
					case 100900:
						{
							BioLinccController biolincc_controller = new BioLinccController(repo, source_id);
							if (create_tables) 
							{
								biolincc_controller.DropADTables(); 
							}
							biolincc_controller.EstablishNewADTables();
							biolincc_controller.LoopThroughRecords();
							break;
						}
					case 100901:
						{
							YodaController yoda_controller = new YodaController(repo, source_id);
							if (create_tables) 
							{
								yoda_controller.DropADTables(); 
							}
							yoda_controller.EstablishNewADTables();
							yoda_controller.LoopThroughRecords();
							break;
						}
				}
			}
		}
	}
}
