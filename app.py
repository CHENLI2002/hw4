# fastapi app
from fastapi import FastAPI
from neo4j import GraphDatabase


external_ip = "35.193.180.181"

driver = GraphDatabase.driver(f"bolt://{external_ip}:7687", auth=("neo4j", "12345678"))


app = FastAPI()

@app.get("/graph-summary")
def graph_summary():
    with driver.session() as session:
        driver_count = session.run("MATCH (d:Driver) RETURN COUNT(d) AS count").single()["count"]
        company_count = session.run("MATCH (c:Company) RETURN COUNT(c) AS count").single()["count"]
        area_count = session.run("MATCH (a:Area) RETURN COUNT(a) AS count").single()["count"]
        trip_count = session.run(
            "MATCH ()-[r:TRIP]->() RETURN count(r) AS count"
        ).single()["count"]
        return {
            "driver_count": driver_count,
            "company_count": company_count,
            "area_count": area_count,
            "trip_count": trip_count,
        }

@app.get("/top-companies")
def topc_companies(n: int):
    with driver.session() as session:
        list_of_companies = session.run("""
            MATCH (a:Driver)-[:WORKS_FOR]->(b:Company) 
            MATCH (a)-[:TRIP]->(c:Area)
            RETURN b.company AS company, COUNT(*) AS count
            ORDER BY count DESC
            LIMIT $n
        """, n=n).data()
        ans = {"companies": []}
        for company in list_of_companies:
            company_name = company["company"]
            count = company["count"]
            ans["companies"].append({
                "name": company_name,
                "trip_count": count,
            })
        return ans

@app.get("/high-fare-trips")
def high_fare_trips(area_id: int, min_fare: float):
    with driver.session() as session:
        list_of_trips = session.run("""
            MATCH (a:Driver)-[t:TRIP]->(b:Area)
            WHERE b.area_id = $area_id AND t.fare > $min_fare
            RETURN t.trip_id AS trip_id, t.fare AS fare, t.trip_seconds AS trip_seconds
        """, area_id=area_id, min_fare=min_fare).data()
        ans = {"trips": []}
        for trip in list_of_trips:
            ans["trips"].append({
                "trip_id": trip["trip_id"],
                "fare": trip["fare"],
                "trip_seconds": trip["trip_seconds"],
            })
        return ans

@app.get("/co-area-drivers")
def co_area_drivers(driver_id: str):
    with driver.session() as session:
        list_of_areas = session.run("""
            MATCH (a:Driver)-[t:TRIP]->(b:Area)
            WHERE a.driver_id = $driver_id
            RETURN b.area_id AS area_id
        """, driver_id=driver_id).data()
        original_areas = set([area["area_id"] for area in list_of_areas])
        
        all_other_drivers = session.run("""
            MATCH (a:Driver)-[t:TRIP]->(b:Area)
            WHERE a.driver_id != $driver_id AND b.area_id IN $original_areas
            RETURN a.driver_id AS driver_id, collect(DISTINCT b.area_id) AS area_ids
        """, driver_id=driver_id, original_areas=original_areas).data()
        ans = {"co_area_drivers": []}
        for other_driver in all_other_drivers:
            here_list = set(other_driver["area_ids"])
            match = here_list & original_areas
            if len(match) > 0:
                ans["co_area_drivers"].append({
                    "driver_id": other_driver["driver_id"],
                    "shared_areas": len(match)
                })
        return ans

def avg_fare_by_company():
    with driver.session() as session:
        all_companies = session.run("""
        MATCH (a:Driver)-[:WORKS_FOR]->(c:Company)
        RETURN c.company AS company, collect(DISTINCT a.driver_id) AS driver_ids
        """).data()

        each_company = {total_fare: 0, trip_count: 0}

        ans = {"companies": []}
        for company in all_companies:
            driver_ids = company["driver_ids"]
            for driver_id in driver_ids:
                trips = session.run("""
                    MATCH (a:Driver)-[t:TRIP]->(b:Area)
                    WHERE a.driver_id = $driver_id
                    RETURN sum(t.fare) AS total_fare, count(*) AS trip_count
                """, driver_id=driver_id).data()
                total_fare = trips[0]["total_fare"]
                trip_count = trips[0]["trip_count"]
                each_company[total_fare] += total_fare
                each_company[trip_count] += trip_count
        for company in each_company:
            ans["companies"].append({
                "name": company["company"],
                "avg_fare": company["total_fare"] / company["trip_count"],
            })
        return ans