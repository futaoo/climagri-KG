# My Project

This project has created an extensive database in climate and agriculture data for the years ranging 2004-2014, presented in the form of a Neo4J knowledge Graph.

## Installation

1. Download the Neo4J Dump file in the project and run in Neo4J Desktop to access and use the graph.

## Description

This project aims to collect and analyze data from various sources to create nodes in Neo4j database related to countries, agriculture, and climate. It includes scripts for data collection, data cleaning, and node creation.

## Files

### Neo4j.py

This script contains functions to connect to the Neo4j database and execute queries.

### country_node_creation.py

This script retrieves country data from a local file and creates country nodes in the Neo4j database.

### data_frame_IO.py

This script provides functions to read and write data frames to/from different file formats (e.g., CSV, Excel).

### faostat_data_collection.py

This script collects agricultural data from the FAOSTAT database using its API.

### faostat_node_creation.py

This script processes the agricultural data collected from FAOSTAT and creates relevant nodes in the Neo4j database.

### main.py

This is the main script that orchestrates the execution of other scripts and controls the workflow of data collection and node creation.

### ncei_cleaner.py

This script performs cleaning operations on climate data obtained from the National Centers for Environmental Information (NCEI) dataset.

### ncei_data_collection.py

This script collects climate data from the NCEI dataset.

### ncei_node_creation.py

This script processes the climate data collected from NCEI and creates nodes in the Neo4j database related to climate.

## Acknowledgements

I would like to thank the following individuals for their contributions and support during the project:

- Jiantao Wu ([@IvanNg](https://csgitlab.ucd.ie/IvanNg))
- Soumyabrata Dev ([@soumya](https://csgitlab.ucd.ie/soumya))

