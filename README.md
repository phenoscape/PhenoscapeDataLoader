# PhenoscapeDataLoader
Data processing utilities for ingest and assembling the legacy Phenoscape knowledgebase underlying http://fish.phenoscape.org, the legacy Phenoscape KB for fishes. It loads data into the OBD-based Phenoscape Knowledgebase (a triple store implemented as a relational SQL database).

The following components are related:
* Web application (providing the web UI): https://github.com/phenoscape/PhenoscapeWeb
* Data services (for delivering data from the OBD database to the web frontend): https://github.com/phenoscape/PhenoscapeOBD-WS
* Ontology-Based Database (OBD) schema and API code: https://github.com/phenoscape/OBDAPI (migrated from [SVN repo](https://sourceforge.net/p/obo/svn/HEAD/tree/OBDAPI/))

_Note that the Fish KB is legacy, and the current incarnation of the KB (covering vertebrates, in contrast to teleost fish only) does not use this software._ 
