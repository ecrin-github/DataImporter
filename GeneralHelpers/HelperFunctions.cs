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
	public static class Helpers
	{
		public static void SendMessage(string message)
		{
			Console.WriteLine(message);
		}

	}
}
