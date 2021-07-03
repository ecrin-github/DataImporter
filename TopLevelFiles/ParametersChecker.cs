using CommandLine;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DataImporter
{
    internal class ParametersChecker : IParametersChecker
    {
        private ILogger _logger;
        private ILoggerHelper _logger_helper;
        private ICredentials _credentials;
        private IMonitorDataLayer _mon_repo;
        private ITestingDataLayer _test_repo;

        public ParametersChecker(ILogger logger, ILoggerHelper logger_helper,
                  ICredentials credentials, IMonitorDataLayer mon_repo,
                  ITestingDataLayer test_repo)
        {
            _logger = logger;
            _logger_helper = logger_helper;
            _credentials = credentials;
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
                _logger.Error(e.Message);
                _logger.Error(e.StackTrace);
                _logger.Information("Harvester application aborted");
                _logger_helper.Logheader("Closing Log");
                return false;
            }

        }


        private void HandleParseError(IEnumerable<Error> errs)
        {
            // log the errors
            _logger.Error("Error in the command line arguments - they could not be parsed");
            int n = 0;
            foreach (Error e in errs)
            {
                n++;
                _logger.Error("Error {n}: Tag was {Tag}", n.ToString(), e.Tag.ToString());
                if (e.GetType().Name == "UnknownOptionError")
                {
                    _logger.Error("Error {n}: Unknown option was {UnknownOption}", n.ToString(), ((UnknownOptionError)e).Token);
                }
                if (e.GetType().Name == "MissingRequiredOptionError")
                {
                    _logger.Error("Error {n}: Missing option was {MissingOption}", n.ToString(), ((MissingRequiredOptionError)e).NameInfo.NameText);
                }
                if (e.GetType().Name == "BadFormatConversionError")
                {
                    _logger.Error("Error {n}: Wrongly formatted option was {MissingOption}", n.ToString(), ((BadFormatConversionError)e).NameInfo.NameText);
                }
            }
            _logger.Information("Harvester application aborted");
            _logger_helper.Logheader("Closing Log");
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

    }
}
