# Sequor
Sequor is an integration system
Sequor is a SQL-centric platform for building API integrations without lock-in and black boxes. It fuses API execution with database allowing data cyrculation from API to database and back to API. Having intermidiate data in the database allows you to transform them, implement analytics and busines logic in SQL. This execution model naturaly works equally well for iPaaS-style app integration and ETL-style data integrations. In Sequor you define flows in YAML with dynamic parametrarization via Jinja templates and Python. Such code-first approace allows you to apply software engineers to integrations such as code versioning and collaboration, CI/CD, local development. Using Sequor you can own your integrations and avoid black boxes via transporate configuration. Control your integrations with code-level precision of SQL and Python. Scale your integrations by easily deploying on premise without SaaS lockin.

Sequor is very easy to learn and you can start fast. Basically, it is just two operations: 
* http_request - executes API calls on top of database: iterate over input records, execute dynamic HTTP calls, and map responses back to tables. Jinja and/or Python is used for dynamics paramters.
* transform - execute SQL query to prepare data for API calls or to process the results

See examples of htt_request and transform operations in action.



  
