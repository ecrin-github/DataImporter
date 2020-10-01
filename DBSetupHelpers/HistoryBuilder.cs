using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataImporter
{
	class HistoryBuilder
	{
		string connstring;
		Source source;
		HistoryTableCreator htc;
		HistoryTableManager htm;

		public HistoryBuilder(string _connstring, Source _source)
		{
			connstring = _connstring;
			source = _source;
			htc = new HistoryTableCreator(connstring);
			htm = new HistoryTableManager(connstring);
		}


		public void CreateHistoryTables()
		{
			htc.CreateHistoryMasterTable();
			if (source.has_study_tables)
			{
				htc.CreateStudyRecsHistoryTable();
				htc.CreateStudyAttsHistoryTable();
				StringHelpers.SendFeedback("Created studies history tables");
			}
			htc.CreateObjectRecsHistoryTable();
			htc.CreateObjectAttsHistoryTable();
			StringHelpers.SendFeedback("Created data object history tables");
		}


		public void RecordImportHistory(ImportEvent import)
        {
			htm.ObtainMasterListState();    // see if previous imports to consider
			if (source.has_study_tables)
			{
				htm.AddStudyRecsAdded();
				htm.AddStudyRecsDeleted();
				htm.ProcessEditedStudyRecs();
				htm.ProcessEditedStudyAtts();
				StringHelpers.SendFeedback("Study history tables updated");
			}
			htm.AddObjectRecsAdded();
			htm.AddObjectRecsDeleted();
			htm.ProcessEditedObjectRecs();
			htm.ProcessEditedObjectAtts();

			htm.CreateAndStoreHistoryRecord(import);
			StringHelpers.SendFeedback("Object history tables updated");
		}

	}
}
