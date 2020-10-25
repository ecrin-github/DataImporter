# DataImporter
Transfers session data into accumulated data tables.

The program takes the data in the session data or sd tables in each source database (the 'session data' being created by the most recent harvest operation), and compares it with the accumulated data for each source, which is stored in the accumulated data (ad) tables. New and revised data are then transferred to the ad tables.<br/>
The program represents the third stage in the 4 stage MDR extraction process:<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Download => Harvest => **Import** => Aggregation<br/><br/>
For a much more detailed explanation of the extraction process,and the MDR system as a whole, please see the project wiki (landing page at https://ecrin-mdr.online/index.php/Project_Overview).<br/>

### Parameters
The system is a console app, (to more easily support being scheduled) and can take takes 2 parameters: <br/>
a) -s followed by a comma separated list of integer ids, each representing a data source within the system. The program takes each of these ids in turn, and carries out the sd to ad import process for each of them.<br/>
b) -T, as a flag. If present, forces the recreation of a new set of accumulated data tables. This parameter should therefore only be used when creating the ad tables for the first time, or when the entire ad schema is recreated from harvested data that represents the totality of the data available from the source. In other words when carrying out a full rebuild of the source's data from a full harvest.<br/><br/>
Thus, the parameter string<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;-s "100116, 100124, 100132" <br/>
will cause data to be imported from the session data tables for each of the Australian, German and Dutch trial registries, in that order.<br/>
The parameter string<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;-s "101900, 101901" -T<br/>
will cause, for the BioLinncc and Yoda repositories respectively, the current ad tables to be dropped and then rebuilt, before being filled with the most recent session data harvested from those repositories.<br/>  
Note there is no parameter to allow using only part of the harvested data in the sd tables - all of that data is used. The way in which different data is imported, e.g. data that has been revised or added after a certain date, is by controlling the data download and harvest processing that precede the import step.<br/>

### Overview
The Import process is the same for all sources, because session and accumulated data tables are structured in a standard way in each source database (though the exact tables that are present may vary between sources).<br/>
The system compares the study data ids and content in the session and accumulated data and identifies a) those studies in the sd tables that are new, b) those studies that have been edited in any way, including any change in a study attribute, c) those studies that are unchanged, and d) those studies that have disappeared from the sd data. The last is relatively rare and can only be estimated if the previous harvest was of 100% of the source.<br/>
The same 4 categories are then constructed for the data objects.<br/> 
For studies that have been edited in any way (this is discovered by comparing composite hash values constructed from the totality of each study's data) the nature of the edit is then identified - i.e. whether or not it involves changes to the main study record, and / or if it involves an addition, edit, or deletion of any study attribute record. Again this process is repeated for the data object data.<br/> 
At the end of this process the system has - within the ad schema of the source database - 4 temporary tables that hold this information, in effect providing a complete record of the differences between the session data and accumulated data sets.<br/> 
<br/>
The sytem then works through the various categories of data that have been identified:
* Any new studies are directly imported into the ad tables along with all their attributes.
* For edited studies, the nature of the edit is examined. If a change has occured in the main (singleton) study record, that record is replaced with the new version from the sd data. If a change has occured in a study attribute, *all* the attribute records of that type are replaced by *all* the attribute records of that type in the sd data. There is no attempt to try and match individual attribute records to see which specifically have been changed / added / deleted - partly because of the lack of persistent identifiers for these records. Instead the whole set of attributes is replaced. If a completely new type of attribute appears for a study all the records of that attribvute type are added. If an attribute type completely disapears from a study all the corresponding attribute records are removed.<br/>
* For unchanged studies the 'date of data fetch' is updated to match that in the sd data but no other changes are applied. This indicates the last date the data was examined, even if the data was unchanged. The same update, of 'date of data fetch' is made to edited records and is contained automatically within new records. 
* For deleted studies, if it has been possible to identify these, the entire study and all attributes are removed from the ad tables.<br/>  
All 4 steps are then repeated for the data object data.<br/> 

### Logging
For each source an 'import event' record is constructed and stored in the monitor database. This details the numbers of new, edited and deleted study and data object records found during the import, as well as its start and end times.<br/> Each individual study record in the monitor database, if classified as new or edited during the import, (or data object record, for sources that contain no studies, such as PubMed) is also updated to indicate exactly when it was last imported into the ad tables. The system therfore knows the 'last import date time' of each source record.<br/>Each individual record in the ad tables also contains a record of when it was created and last edited.<br/>

### Provenance
* Author: Steve Canham
* Organisation: ECRIN (https://ecrin.org)
* System: Clinical Research Metadata Repository (MDR)
* Project: EOSC Life
* Funding: EU H2020 programme, grant 824087
