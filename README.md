# Introduction

This repository is part of the development of the final project of the Indicium Lighthouse Program. The project involved using Adventure Works databased to create a Data Warehouse of sales information to help business stakeholders make better informed decisions. After creating de DW and modeling the data, the final star schema model was used to created a dashboard used by operational team and a relatory to the CEO. The final step of the project involved using the data modeled to predict sales demand as a POC to the planning team of ADW. 

Here we have the modeling step of the project. With the development of a Data Warehouse using dbt core connected to BigQuery that was chosen as the main plataform to hold the data.

To see the Advance Analytics part of the project (which includes both dashboard and sales prediction please refer to [this specific repository](https://github.com/gabiskli/adw-advanced-analytics).

# Folder Structures

The structured used for this project was done based on the premiss that other project can be done using this database.

## Staging

This folder don't have any suubfolders and hold all primary files that were used to begin the project. Also, documentation was done in a single file named `sources.yml` where all data sources were called and documented. 

If a more complex filter was done in this stage I chose to document in a `.yml` file with the same name of the coding file (eg. products table). 

Stging tables were materialized as views.

## Intermediate

Here I started dividing the model in different subfolders. I created a *commercial** folder to keep all files related to this department separated from other possible departments.

In this folder each code file is accompanied by a `.yml` file in order to keep all transformations documented correctly. Files here have another level of complexity and are used to perform join, calculation of metrics and other analysis.

Intermediated tables were materialized as ephemeral.

## Marts

Here I used the same divison of folder done in the intermediate level. This is done to keep information of different departments in different enviroments. This way we can restric access more easily if needed and keeps the project organized.

The idea here is to keep our star schema tables (dimension and fact tables) as well as aggregation that could be useful to the project. Again, all code files are accompanied by its documentation file.

Marts tables are materialized as tables.

# Data Sources

The data sources were loaded from seeds files that were in the [original repository](https://github.com/techindicium/academy-dbt) that was forked to make this one. Instruction on how to load table into the database are int the readme file of the presented repository.
