using static System.Console;
using CommandLine;
using System.Collections.Generic;
using System.Linq;

namespace DataImporter
{
	class Program
	{
		static void Main(string[] args)
		{
			var parsedArguments = Parser.Default.ParseArguments<Options>(args)
			.WithParsed(RunOptions)
			.WithNotParsed(HandleParseError);
		}


		private static void RunOptions(Options opts)
		{
			LoggingDataLayer logging_repo = new LoggingDataLayer();
			Importer imp = new Importer();

			if (opts.source_ids.Count() > 0)
			{
				foreach (int source_id in opts.source_ids)
				{
					Source source = logging_repo.FetchSourceParameters(source_id);
					if (source == null)
					{
						WriteLine("Sorry - the first argument does not correspond to a known source");
					}
					else
					{
						imp.Import(source, opts.build_tables);
					}
				}
			}
		}

		private static void HandleParseError(IEnumerable<Error> errs)
		{
			// handle errors
		}
	}


	public class Options
    {
	// Lists the command line arguments and options

	[Option('s', "source_ids", Required = true, Separator = ',', HelpText = "Comma separated list of Integer ids of data sources.")]
	public IEnumerable<int> source_ids { get; set; }

	
	[Option('T', "build tables", Required = false, HelpText = "If present, forces the (re)creation of a new set of ad tables")]
	public bool build_tables { get; set; }

	/*
    [Option('t', "harvest_type_id", Required = true, HelpText = "Integer representing type of harvest (1 = full, 2 = with cutoff date, 3 = incomplete files only).")]
	public int harvest_type_id { get; set; }

	[Option('d', "cutoff_date", Required = false, HelpText = "Only data revised or added since this date will be considered")]
	public string cutoff_date { get; set; }
	*/

   }
}
