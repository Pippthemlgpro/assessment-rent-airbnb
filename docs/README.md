# RevoData's Technical Assessment

### Goal of the Assessment

The aim is to identify postcodes with investment potential and determine whether renting properties long-term through Kamernet or via Airbnb would be more profitable.

## Engineering Deliverables

If you are applying for an engineering role, you must at minimum build one or more data pipelines that:

- Ingest rental data scraped from Kamernet (`./data/rentals.json`)
- Ingest data from Airbnb (`./data/airbnb.csv`)
- Clean both datasets
- Identify and address any missing or improper data by designing (and implementing if time permits) a backfill
  strategy
- Calculate potential revenue per property and per postal code for both rental and Airbnb sources
- Follow the principles of the [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture#:~:text=A%20medallion%20architecture%20is%20a%20data%20design%20pattern,%28from%20Bronze%20%E2%87%92%20Silver%20%E2%87%92%20Gold%20layer%20tables%29.)

The following deliverables are expected as part of the project:

- Exploratory notebooks with data validation checks (placed in the `./scratch` folder)
- Notebooks containing your pipeline implementation (located in the `./notebooks` folder)
- Libraries or buildable packages for your pipeline (stored in `./src/<package_name>`)
- Unit tests for your pipeline (included in the `./tests` folder)
- Documentation for your pipeline (documented in the `README` and `./docs` folders) - **explain the why, not the how**
- Export of datasets produced by your pipeline, formatted as Parquet files (placed in `./data/output`)
- Pipeline job configurations (included in the `./resources` folder)



### Implementation Steps and know-how

1. Implementation follows the medallion architechture

   1. All data lands in bronze as-is

   2. Cleaned and enrished in silver

   3. Utilized and can be forwarded for reporting / visualization

2. The whole pipeline is SQL as I'm more hands on with SQL.

   1. This mirrors my experience in SQL-centric migrations, also lowers maintenance overhead for "legacy" system experts and SQL-proficient teams. Also DBR Photon engine is a nice addition even if a lot of business might not be aware of this.

   2. Point here is the fact that I participated in migrations what was shift and lift. Those were more of a design choise rather than recreating stuff.

3. I wanted to include geojson data, because I never worked with one and by the looks as I checked it would enrich the data at gold hence even if it's a higher level I wanted to add this to the pipeline.

## Landing

1. Even tough github is connected with DBR, files are processed in a dedicated landing volume under bronze

   1. Dedicated volume allows DBR admins to manage the files and check them.

   2. See when the last file arrived

   3. Potetnially create streaming or a trigger for a job if a file arrives in the location

For my configuration the files should land here and the pipeline triggers afterwards potetnially loading the latest files.

## Bronze

1. Bronze contains the files loaded as-is. Nothing changed apart from timestamp addition to see when it got loaded along with source file.

   1. aribnb preserved as string to ensure all data arrives.

   2. JSON is exploded a little to flatten out data.

2. Notebook use full refresh / recreate instead of append only or incremental load due to my experience on multiple other projects. Clients I used to worked with loved to recreate everything to make sure all data is updated instead of adding logic to ensure proper incremental loading. Also landing the files (if not every 10 minutes or so) would be okay to have this. (If file is hundreds or millions of records definietly avoid this.)

## Silver

1. Data is cleaned, deduplicated, enriched with geo data and fixed for downstream reporting.

   1. Utilized regex to find data patterns 

   2. Aligned airbnb and rentals data postal codes and rooms sizes 

   3. Flagged a bunch of stuff simply to ensure flag level filtering if needed for gold.(Some of the clients I worked with loved to redesigns their dashboards every other week. Business issue? Yes. Flagging the data so easier to modify the dashbaord? Yes.)

## Gold

1. As rental and airbnb data is now somewhat aligned, 1-1 comparison can be done.

   1. rentals tipically houses, airbnb tipically rooms. Some areas would be better to have rentals for long term some would be better for airbnb depending on size and cost. This has been categorized.

2. Variables used in case something needs to be chnaged. Based on Gemini, starting 2026 april some law would be enforced in Amsterdam rentals but since the data is from 2020 tops, I would not feel fair to take this into account. Can be done tough. 

3. Same for revenue scenarios in case occupancy statistics changes over time.

4. As data is getting paired the reporting layer would show as insufficient data in order to have at least an insight what is happening.

5. Tables are used for faster data feeding to viz tools. View is present for PC6 codes in case *more* details are needed.

## Export

1. Exporting the files as it was required. 

2. Also, nice to have an export *for a disaster recovery too*. (Obviously if for any reason time travel is not sufficient) My experience - sadly - is the fact that many DR drills utilize copies to an extrernal storage.

## Pipeline

1. Simple DBR job which calls the notebooks one by one.

   1. Easier to understand what is going on.

   2. Where the data ends up if something is wrong.

   3. Alerts configured.

   4. Starts when a new file lands. If at all.

## AI usage

No Claude code was utilized for this assessment (Maybe it's obvious) because I wanted to understand the business logic instead of reading a wall of text if a decision is required. 

Gemini Notebook was the main tool to use. Have a context and chat there about the tasks at hand. Coding, domain knowledge if needed etc. Export uploaded.

Code is AI written and looked over during implementation.

Documentation is hand crafted with AI overlook to not miss anything as implementation created. It's time consuming but 1. I was a librarian. 2. Good documentation is the pillar of AI usage at a company. (Where documentation is present at all.)

## Known bottlenecks

1. Same File names could cause issues in landing if the content is different. 

2. Header change in .csv or in json tree would lead bronze to major schema issues. But eventually stop and silver.

3. Regex utilization. Fragile in case business logic changes or source data changes.
