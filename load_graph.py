from neo4j import GraphDatabase
from pandas import read_csv

external_ip = "35.193.180.181"

driver = GraphDatabase.driver(f"bolt://{external_ip}:7687", auth=("neo4j", "12345678"))

# test connection
with driver.session() as session:
    df = read_csv("taxi_trips_clean.csv")
    uniqu_areas = df["pickup_area"].unique().tolist()
    unique_areas = df["dropoff_area"].unique().tolist()
    area_set = set(unique_areas + unique_areas)
    for area in area_set:
        session.run("MERGE (a:Area {area_id: $area_id})", area_id=area)

    for index, row in df.iterrows():
        # headers are :trip_id,driver_id,company,pickup_area,dropoff_area,fare,trip_seconds
        trip_id = row["trip_id"]
        driver_id = row["driver_id"]
        company = row["company"]
        pickup_area = row["pickup_area"]
        dropoff_area = row["dropoff_area"]
        fare = row["fare"]
        trip_seconds = row["trip_seconds"]

        session.run(
            """
            MERGE (driver:Driver {driver_id: $driver_id})
            MERGE (comp:Company {company: $company})
            MERGE (driver)-[:WORKS_FOR]->(comp)
            WITH driver
            MATCH (area:Area {area_id: $dropoff_area})
            MERGE (driver)-[r:TRIP {trip_id: $trip_id}]->(area)
            SET r.fare = $fare, r.trip_seconds = $trip_seconds
            """,
            driver_id=driver_id,
            company=company,
            dropoff_area=dropoff_area,
            trip_id=trip_id,
            fare=fare,
            trip_seconds=trip_seconds,
        )

driver.close()

"""
Node Label	Properties
:Driver	driver_id (string)
:Company	name (string)
:Area	area_id (integer)
Relationship	Direction	Properties
:WORKS_FOR	(Driver)→(Company)	(none)
:TRIP	(Driver)→(Area)	trip_id (string), fare (float), trip_seconds (int)
Use MERGE on nodes to avoid duplicates. A :TRIP relationship represents a driver making a trip to a dropoff area.
"""
