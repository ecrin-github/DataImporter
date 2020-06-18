# DataImporter
Transfers session data into the accumulated data tables.

The program takes the data in the sd tables in each source database (the 'session data' created by the most recent harvest operation), and compares it with the accumulated data for each source, which is stored in the ad tables. New and revised data is both transferred to the ad tables, and marked as such so that it will be used in the later aggregation phase. 

### Parameters
The system is currently a console app, and takes a single parameter. This is a 6 digit integer representing the source (e.g. 100120 is Clinical Trials.gov). The plan is to wrap the app in a UI at some later point.

### Data Status codes
Data in the accumulated data (ad) tables carries audit and status fields tro inidcate if it is new or has been revised. The codes are:<br/>
0: No change
All files in the source data folder will be converted into data in the sd tables. Used for relatively small sources and / or those that have no 'last revised date'

2: Harvest revised since (cutoff date)
Processes only files that have a 'last revised date' greater than the cutoff date given. Harvests of this type therefore require a third parameter to be supplied.

3: Harvest those considered not completed
Processes those files thast are marked as 'incomplete' in the logging system and ignores those marked as 'comnplete'. The latter designation is sometimes given to files that, whilst they do not contain a date last revised attribute, are old enough and seem to contain sufficient data that any further editing seems very unlikely. Note that even files that are 'complete', however, can ne periodically examined (e.g. on an annual basis) by over-riding the default download and harvest ssettings.

Provenance
Author: Steve Canham
Organisation: ECRIN (https://ecrin.org)
System: Clinical Research Metadata Repository (MDR)
Project: EOSC Life
Funding: EU H2020 programme, grant 824087
