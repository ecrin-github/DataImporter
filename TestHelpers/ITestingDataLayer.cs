using System;
using System.Collections.Generic;
using System.Text;

namespace DataImporter
{
    public interface ITestingDataLayer
    {
        Credentials Credentials { get; }

        IEnumerable<int> ObtainTestSourceIDs();

        void SetUpADCompositeTables();
        void RetrieveSDData(ISource source);
        void RetrieveADData(ISource source);
        void TransferADDataToComp(ISource source);
        void ApplyScriptedADChanges();
        void ConstructDiffReport();
    }
}
