using CommandLine;

using System;
using System.Collections.Generic;
using System.Linq;

namespace DataImporter
{
    internal class ParametersChecker : IParametersChecker
    {
        private LoggingHelper _logging_helper;
        private IMonitorDataLayer _mon_repo;
        private ITestingDataLayer _test_repo;

        public ParametersChecker(IMonitorDataLayer mon_repo, ITestingDataLayer test_repo)
        {
            _mon_repo = mon_repo;
            _test_repo = test_repo;
        }

        // Parse command line arguments and return true only if no errors.
        // Otherwise log errors and return false.

        public Options ObtainParsedArguments(string[] args)
        {
            var parsedArguments = Parser.Default.ParseArguments<Options>(args);
            if (parsedArguments.Tag.ToString() == "NotParsed")
            {
                HandleParseError(((NotParsed<Options>)parsedArguments).Errors);
                return null;
            }
            else
            {
                return ((Parsed<Options>)parsedArguments).Value;
            }
        }

        // Parse command line arguments and return true if values are valid.
        // Otherwise log errors and return false.

        public bool ValidArgumentValues(Options opts)
        {
            try
            {
                if (opts.using_test_data)
                {
                    // Set up array of source ids to reflect
                    // those in the test data set.

                    opts.source_ids = _test_repo.ObtainTestSourceIDs();
                    return true;     // Should always be able to run
                }
                else if (opts.create_test_report)
                {
                    return true;     // Should always be able to run
                }
                else
                { 
                    if (opts.source_ids.Count() == 0)
                    {
                        throw new ArgumentException("No source id provided");
                    }

                    foreach (int source_id in opts.source_ids)
                    {
                        if (!_mon_repo.SourceIdPresent(source_id))
                        {
                            throw new ArgumentException("Source argument " + source_id.ToString() +
                                                        " does not correspond to a known source");
                        }
                    }
                    return true;    // Got this far - the program can run!
                }
            }

            catch (Exception e)
            {
                _logging_helper = new LoggingHelper("no source");
                _logging_helper.LogHeader("INVALID PARAMETERS");
                _logging_helper.LogCommandLineParameters(opts);
                _logging_helper.LogCodeError("Importer application aborted", e.Message, e.StackTrace);
                _logging_helper.CloseLog();
                return false;
            }

        }


        private void HandleParseError(IEnumerable<Error> errs)
        {
            // log the errors

            _logging_helper = new LoggingHelper("no source");
            _logging_helper.LogHeader("UNABLE TO PARSE PARAMETERS");
            _logging_helper.LogHeader("Error in input parameters");
            _logging_helper.LogLine("Error in the command line arguments - they could not be parsed");

            int n = 0;
            foreach (Error e in errs)
            {
                n++;
                _logging_helper.LogParseError("Error {n}: Tag was {Tag}", n.ToString(), e.Tag.ToString());
                if (e.GetType().Name == "UnknownOptionError")
                {
                    _logging_helper.LogParseError("Error {n}: Unknown option was {UnknownOption}", n.ToString(), ((UnknownOptionError)e).Token);
                }
                if (e.GetType().Name == "MissingRequiredOptionError")
                {
                    _logging_helper.LogParseError("Error {n}: Missing option was {MissingOption}", n.ToString(), ((MissingRequiredOptionError)e).NameInfo.NameText);
                }
                if (e.GetType().Name == "BadFormatConversionError")
                {
                    _logging_helper.LogParseError("Error {n}: Wrongly formatted option was {MissingOption}", n.ToString(), ((BadFormatConversionError)e).NameInfo.NameText);
                }
            }
            _logging_helper.LogLine("Importer application aborted");
            _logging_helper.CloseLog();
        }
    }


    public class Options
    {
        // Lists the command line arguments and options

        [Option('s', "source_ids", Required = false, Separator = ',', HelpText = "Comma separated list of Integer ids of data sources.")]
        public IEnumerable<int> source_ids { get; set; }

        [Option('T', "build tables", Required = false, HelpText = "If present, forces the (re)creation of a new set of ad tables")]
        public bool rebuild_ad_tables { get; set; }

        [Option('F', "is a test", Required = false, HelpText = "If present, operates on the sd / ad tables in the test database")]
        public bool using_test_data { get; set; }

        [Option('G', "test report", Required = false, HelpText = "If present, compares and reports on adcomp and expected tables but does not recreate those tables")]
        public bool create_test_report { get; set; }
    }
}
