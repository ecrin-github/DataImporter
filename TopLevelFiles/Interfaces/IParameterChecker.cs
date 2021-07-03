using System;
using System.Collections.Generic;
using System.Text;

namespace DataImporter
{
    interface IParametersChecker
    {
        internal interface IParametersChecker
        {
            Options ObtainParsedArguments(string[] args);
            bool ValidArgumentValues(Options opts);
        }
    }
}
