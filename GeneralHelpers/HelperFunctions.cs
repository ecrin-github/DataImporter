using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Reflection.Metadata.Ecma335;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace DataImporter
{
	public static class StringHelpers
	{
		public static void SendFeedback(string message, string identifier = "")
		{
			string dt_string = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
			System.Console.WriteLine(dt_string + message + identifier);
		}

	}
}
