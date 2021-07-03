using System;
using System.Collections.Generic;
using System.Text;

namespace DataImporter
{
    public interface ICredentials
    {
        string Host { get; set; }
        string Password { get; set; }
        string Username { get; set; }

        string GetConnectionString(string database_name, bool using_test_data);
    }
}
