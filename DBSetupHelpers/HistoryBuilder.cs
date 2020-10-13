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
			htc.CreateToAggImportTable();
			if (source.has_study_tables)
			{
				htc.CreateToAggStudyRecsTable();
				htc.CreateToAggStudyAttsTable();
				StringHelpers.SendFeedback("Created studies to_agg tables");
			}
			htc.CreateToAggObjectRecsTable();
			htc.CreateToAggObjectAttsTable();
			StringHelpers.SendFeedback("Created data object to_agg tables");
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
				StringHelpers.SendFeedback("Study history to_agg updated");
			}
			htm.AddObjectRecsAdded();
			htm.AddObjectRecsDeleted();
			htm.ProcessEditedObjectRecs();
			htm.ProcessEditedObjectAtts();

			htm.CreateAndStoreHistoryRecord(import);
			StringHelpers.SendFeedback("Object to_agg tables updated");
		}

		public ImportEvent CreateImportEvent(int import_id)
		{
			return htm.CreateImportEvent(import_id, source);
		}

	}
}
