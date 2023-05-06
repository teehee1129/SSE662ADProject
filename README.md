# SSE 662 Alisha and David's Project

This serves as the repository for all of the projects regarding class SSE 662 - Design Maintenance and Quality.

Project Description:
Mercer Libraries maintains a list of active users for the Library Management System of active patrons. This is required to limit access to currently enrolled students or employed staff and faculty. The list is used to grant access to the library's resources that not only include physical books, but also access to digital resources including books, journals, and videos.

The library receives daily updates in the form of CSV files that include active students and current employees. The program that is used for the course project is a script that takes the active student list and loads it into a PostgreSQL database for further processing.


The motivation behind working on this code was due to needing to update it recently to make it work on a new server. The new server is running a newer operating system and database software version. A transition from Python 2 to 3 was part of the server upgrade. This necessitated updates to the program to work with the newer software.

For the sake of this project, a test copy of the database has been available. The server providing the database is a test server residing behind one of the library’s public-facing web servers. Please be aware that due to the university’s firewall rules, access to this server is limited to either on campus access or through the university’s VPN.

Project Objectives:

Connect to temporary database

Receive input of credentials

Input data from a CSV file into the database

Export data from the database into an XML file
