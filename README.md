# DataImporter
Transfers session data into the accumulated data tables.

The program takes the data in the sd tables in each source database (the 'session data' created by the most recent harvest operation), and compares it with the accumulated data for each source, which is stored in the ad tables. New and revised data is both transferred to the ad tables, and marked as such so that it will be used in the later aggregation phase. 

### Parameters
The system is currently a console app, and takes a single parameter. This is a 6 digit integer representing the source (e.g. 100120 is Clinical Trials.gov). Each source system needs to be imnported in turn before aggregation begins, though the exact ordering is not important. The plan is to wrap the app in a UI at some later point.

### Data Status codes
Data in the accumulated data (ad) tables carries audit and status fields tro inidcate if it is new or has been revised. The codes are:<br/>
0: No change<br/>
*Applies to all record types. Indicates a) that the record was not present in the sd data and is assumed to be unchanged, and b) has already been transferred to the central mdr in an earlier aggregation. Any record with status 0 can be ignored in any later aggregation phase.*

1: New data<br/>
*Applies to all record types. Indicates that the record has been newly added from the session data, and that it must therefore be further added to the central systems in any later aggregation process. The provenance data of the parent Study or Data Object is also included.*

2: Revised data<br/>
*Applies to all record types. Indicates that the record has been revised in some way, and that the version in the ad tables has therefore replaced the version that was there previously. That revision needs to be passed onto the central mdr tables. Hashes are used extensively in the system to more easily identify the changed components.*

3: Data confirmed as unchanged<br/>
*Applies only to Study and Data Object records - i.e. the 'main' record for each and not their associated attribute records. Indicates that a) that the record **was** present in the sd data but b) that the study (including all of its attributes) oir the data object (including all of its attributes) was unchanged. The only thing that needs to be changed is therefore the 'data of data download' field in the Study or Data Object record, corresponding to the date of the most recent download. This date needs to be revised, which will then trigger a revision of the record's provenance data.*


### Provenance
Author: Steve Canham
Organisation: ECRIN (https://ecrin.org)
System: Clinical Research Metadata Repository (MDR)
Project: EOSC Life
Funding: EU H2020 programme, grant 824087
