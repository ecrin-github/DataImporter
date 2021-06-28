using CommandLine;
using System;
using System.Collections.Generic;
using System.Linq;
using static System.Console;

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
            try
            {
                if (opts.source_ids.Count() > 0)
                {
                    foreach (int source_id in opts.source_ids)
                    {
                        DataLayer repo = new DataLayer(source_id, opts.is_test);
                        if (repo.Source == null)
                        {
                            WriteLine("Sorry - the first argument does not correspond to a known source");
                        }
                        else
                        {
                            imp.Import(repo, logging_repo, opts.build_tables);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                logging_repo.LogError("Unhandled exception: " + e.Message);
                logging_repo.LogLine(e.StackTrace);
                logging_repo.LogLine(e.TargetSite.Name);
                logging_repo.CloseLog();
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

    [Option('X', "is a test", Required = false, HelpText = "If present, operates on the sd / ad tables in the test database")]
    public bool is_test { get; set; }

    }
}
