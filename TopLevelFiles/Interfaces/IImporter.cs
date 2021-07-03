using System;
using System.Collections.Generic;
using System.Text;

namespace DataImporter
{
    public interface IImporter
    {
        int Run(Options opts);

    }
}
