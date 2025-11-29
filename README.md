# Blitzen

1. Project Description

We’re building an ETL pipeline that ingests and integrates annual College Scorecard and IPEDS data into a unified database, tracking institutional details, admissions, finances, and outcomes over time.

Data Sources:
College Scorecard Data (Annually): Institution-level tuition data, degree breakdown, testing and admissions data, demographic completion rate information, and earnings data.
IPEDS Data (Annually): Institution-specific data such as name, location data, and Carnegie Classification scores.


2. Documentation & File Structure

CollegeScorecardDataDictionary is the data dictionary for the data in case there is a need to reference the variables, dataframes, etc.
credentials_copy.py is important for storing the user's personal username and database password. This file needs to be changed to have the user's appropriate credentials prior to running any other files.
part_two.ipynb is the code to create the tables based on our SQL schema, which should be run before the load files.
load-ipeds.py is the python script for loading the IPEDS data. This should be run, after the part_two file is run, in the terminal to insert the IPEDS data into the SQL tables.
load-scorecard.py is the python script for loading the College Scorecard data. This should be run last in the terminal to insert the College Scorecard data into the SQL tables.
part_one.ipynb and part_one.html are the Jupyter notebook and HTML files associated with our initial table schema, and should be ignored in favor of the improved schema design in part_two.ipynb

3. Summary of Instructions to Run Files

i. Create, or update, the credentials_copy.py file to have the user's appropriate username and database password assigned to variables.

ii. Run part_two.ipynb to connect to the database and run the CREATE TABLE SQL statements.

iii. In the terminal, run 'python load-ipeds.py [filename]' to load in the IPEDS data and insert into the relevant SQL tables. These are the files that are called 'hd'.

iv. Finally, in the terminal again, run 'python load-scorecard.py [filename]' to load in the College Scorecard data and insert into the relevant SQL tables. These are the files that are called 'merged'.

