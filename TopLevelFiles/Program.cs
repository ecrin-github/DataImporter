﻿using System;
using System.IO;
using System.Reflection;
using Serilog;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace DataImporter
{
    class Program
    {
        static void Main(string[] args)
        {
            //var parsedArguments = Parser.Default.ParseArguments<Options>(args)
            //.WithParsed(RunOptions)
            //.WithNotParsed(HandleParseError);

            // Set up file based configuration environment.

            var configFiles = new ConfigurationBuilder()
                .SetBasePath(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location))
                .AddJsonFile("appsettings.json")
                .AddJsonFile($"appsettings.{Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")}.json", true)
                .Build();

            // Set up the host for the app,
            // adding the services used in the system to support DI
            // and including Serilog

            IHost host = Host.CreateDefaultBuilder()
                 .UseContentRoot(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location))
                 .ConfigureAppConfiguration(builder =>
                 {
                     builder.AddConfiguration(configFiles);
                 })
                 .ConfigureServices((hostContext, services) =>
                 {
                     // Register services (or develop a comp root)
                     services.AddSingleton<ICredentials, Credentials>();
                     services.AddSingleton<IParametersChecker, ParametersChecker>();
                     services.AddSingleton<ILoggerHelper, LoggerHelper>();
                     services.AddSingleton<IImporter, Importer>();
                     services.AddSingleton<IMonitorDataLayer, MonitorDataLayer>();
                     services.AddSingleton<ITestingDataLayer, TestingDataLayer>();
                     services.AddTransient<ISource, Source>();
                 })
                 .UseSerilog(new LoggerConfiguration()
                        .ReadFrom.Configuration(configFiles)
                        .CreateLogger())
                 .Build();

            // Check the command line arguments to ensure they are valid,
            // If they are, start the program by instantiating the Harvester object
            // and telling it to run. The parameter checking process also creates
            // a singleton monitor repository.

            ParametersChecker paramChecker = ActivatorUtilities
                                    .CreateInstance<ParametersChecker>(host.Services);
            Options opts = paramChecker.ObtainParsedArguments(args);

            if (opts != null && paramChecker.ValidArgumentValues(opts))
            {
                Importer importer = ActivatorUtilities.CreateInstance<Importer>(host.Services);
                Environment.ExitCode = importer.Run(opts);
            }
        }
    }

}
