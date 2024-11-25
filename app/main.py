from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from app import database, schemas
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional, Dict, Any
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
import math

from datetime import datetime

app = FastAPI()

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

origins = [
    "http://localhost:4000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permet toutes les origines
    allow_credentials=True,
    allow_methods=["*"],  # Permet tous les types de méthodes HTTP
    allow_headers=["*"],  # Permet tous les types d'en-têtes
)


def get_db():
    db = database.SessionLocal()
    try:
        yield db
    finally:
        db.close()

def common_query_params(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    slug: Optional[str] = Query(None),
    offer: Optional[int] = Query(None),
    source: Optional[List[str]] = Query(None),
    dest_code: Optional[str] = Query(None),
    currency_code: Optional[str] = Query(None),
    adjustement_type: Optional[str] = Query(None),
    source_request: Optional[str] = Query(None),
    isb2b: Optional[bool] = Query(None),
    req_type: Optional[str] = Query(None),
):
    return {
        "start_date": start_date,
        "end_date": end_date,
        "slug": slug,
        "offer": offer,
        "source": source,
        "dest_code": dest_code,
        "currency_code": currency_code,
        "adjustement_type": adjustement_type,
        "source_request": source_request,
        "isb2b": isb2b,
        "req_type": req_type,
    }

@app.get("/growth_kpi/", response_model=schemas.GrowthKPI)
def get_growth_kpi(db: Session = Depends(get_db)):
    gain_2022_query = text("""
        SELECT COALESCE(SUM(f.total_price - f.offer_price) - MIN(f.prix_annuel), 0) as gain
        FROM factrequests f
        JOIN dimdates d ON f.date_fk = d.date_pk
        WHERE d."Annee" = 2022
    """)
    
    gain_2023_query = text("""
        SELECT COALESCE(SUM(f.total_price - f.offer_price) - MIN(f.prix_annuel), 0) as gain
        FROM factrequests f
        JOIN dimdates d ON f.date_fk = d.date_pk
        WHERE d."Annee" = 2023
    """)
    
    gain_2024_query = text("""
        SELECT COALESCE(SUM(f.total_price - f.offer_price) - MIN(f.prix_annuel), 0) as gain
        FROM factrequests f
        JOIN dimdates d ON f.date_fk = d.date_pk
        WHERE d."Annee" = 2024
    """)
    
    gain_2022 = db.execute(gain_2022_query).scalar()
    gain_2023 = db.execute(gain_2023_query).scalar()
    gain_2024 = db.execute(gain_2024_query).scalar()
    
    if gain_2022 is None or gain_2023 is None or gain_2024 is None:
        raise HTTPException(status_code=404, detail="Data not found")
    
    growth_2023 = ((gain_2023 - gain_2022) / gain_2022) *100 if gain_2023 != 0 else 0
    growth_2024 = ((gain_2024 - gain_2023) / gain_2023) *100  if gain_2024 != 0 else 0
    
    return schemas.GrowthKPI(
        gain_2022=gain_2022, 
        gain_2023=gain_2023, 
        gain_2024=gain_2024, 
        growth_2023=growth_2023, 
        growth_2024=growth_2024
    )

@app.get("/factrequests/gain", response_model=schemas.FactRequestGain)
def read_factrequest_gain(
    db: Session = Depends(get_db),
    params: Dict[str, Any] = Depends(common_query_params)
):
    query = """
        SELECT 
            cl.customer as customer_name, 
            SUM(total_price - offer_price) as total_gain
        FROM factrequests f
        JOIN dimdates d ON f.date_fk = d.date_pk
        JOIN dimcars c ON f.car_fk = c.car_pk
        JOIN dimClients cl ON f.client_fk = cl.client_pk
    """
    
    filters = "WHERE 1=1"
    if params.get("start_date") and params.get("end_date"):
        filters += " AND d.date BETWEEN :start_date AND :end_date"
    if params.get("slug"):
        filters += " AND c.slug = :slug"
    
    query = query + filters + " GROUP BY cl.customer"
    
    query += " ORDER BY total_gain DESC LIMIT 10"
    
    results = db.execute(text(query), params).fetchall()
    
    if not results:
        raise HTTPException(status_code=404, detail="Data not found")
    
    data = [
        schemas.FactRequestGainData(
            gain=round(row.total_gain, 3),
            customer_name=row.customer_name.strip()
        )
        for row in results
    ]
    
    return schemas.FactRequestGain(data=data)




@app.get("/brand-gains/", response_model=schemas.BrandGainsKPI)
def get_car_type_gains(
    db: Session = Depends(get_db),
    params: Dict[str, Any] = Depends(common_query_params)
):
    base_query = """
        SELECT 
            c.brand as brand,
            SUM(f.total_price - f.offer_price) as total_gain
        FROM 
            factrequests f
        JOIN
            dimcars c ON f.car_fk = c.car_pk
    """

    filters = "WHERE 1=1"
    if params.get("slug"):
        filters += " AND c.slug = :slug"
    if params.get("start_date") and params.get("end_date"):
        filters += " AND f.pick_up_date::date BETWEEN :start_date AND :end_date"

    final_query = base_query + filters + " GROUP BY c.brand"
    
    try:
        results = db.execute(text(final_query), params).fetchall()
        
        if not results:
            raise HTTPException(status_code=404, detail="Data not found")
        
        data = [
            {
                "brand": row.brand.strip(), 
                "total_gain": round(row.total_gain,2)
            }
            for row in results
        ]
        
        return schemas.BrandGainsKPI(data=data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    
@app.get("/car_clients_kpi/", response_model=schemas.CarClientsKPI)
def get_car_clients_kpi(
    db: Session = Depends(get_db),
    params: Dict[str, Any] = Depends(common_query_params)
):
    query = """
        SELECT
            c.brand,
            r.req_type,
            COUNT(cl.client_pk) as client_count
        FROM factrequests f
        JOIN dimcars c ON f.car_fk = c.car_pk
        JOIN dimclients cl ON f.client_fk = cl.client_pk
        JOIN dimrequesttypes r ON f.req_type_fk = r.req_type_pk
    """
    
    filters = "WHERE 1=1"
    if params.get("slug"):
        filters += " AND c.slug = :slug"
    if params.get("start_date") and params.get("end_date"):
        filters += " AND f.pick_up_date::date BETWEEN :start_date AND :end_date"

    query = query + filters + " GROUP BY c.brand, r.req_type"
    
    results = db.execute(text(query), params).fetchall()
    
    if not results:
        raise HTTPException(status_code=404, detail="Data not found")
    
    data = [
        schemas.CarClientsData(
            brand=row.brand.strip(),
            req_type=row.req_type.strip(),
            client_count=row.client_count
        )
        for row in results
    ]
    
    return schemas.CarClientsKPI(data=data)

@app.get("/car_owners_kpi/", response_model=schemas.CarOwnersKPI)
def get_car_owners_kpi(
    db: Session = Depends(get_db),
    params: Dict[str, Any] = Depends(common_query_params)
):
    car_count_query = """
        SELECT
            COALESCE(owner, 'Unknown') as owner,
            COUNT(car_pk) as car_count
        FROM dimcars
    """
    
    filters = "WHERE 1=1"
    if params.get("slug"):
        filters += " AND slug = :slug"

    car_count_query = car_count_query + filters + " GROUP BY COALESCE(owner, 'Unknown')"

    request_count_query = """
        SELECT
            COALESCE(c.owner, 'Unknown') as owner,
            COUNT(f.req_pk) as request_count
        FROM factrequests f
        JOIN dimcars c ON f.car_fk = c.car_pk
    """
    
    filters = "WHERE 1=1"
    if params.get("slug"):
        filters += " AND c.slug = :slug"

    request_count_query = request_count_query + filters + " GROUP BY COALESCE(c.owner, 'Unknown')"

    total_requests_query = "SELECT COUNT(*) as total_requests FROM factrequests"

    car_count_results = db.execute(text(car_count_query), params).fetchall()
    request_count_results = db.execute(text(request_count_query), params).fetchall()
    total_requests = db.execute(text(total_requests_query)).scalar()

    if not car_count_results or not request_count_results or total_requests is None:
        raise HTTPException(status_code=404, detail="Data not found")

    request_count_map = {row.owner: row.request_count for row in request_count_results}
    data = [
        schemas.CarOwnersData(
            owner=row.owner.strip(),
            car_count=row.car_count,
            request_percentage=(request_count_map.get(row.owner, 0) / total_requests) * 100
        )
        for row in car_count_results
    ]

    return schemas.CarOwnersKPI(data=data)


@app.get("/requests_per_benchmark_by_source_and_region/", response_model=schemas.RequestsPerBenchmarkBySourceAndRegion)
def get_requests_per_benchmark_by_source_and_region(
    db: Session = Depends(get_db),
    params: Dict[str, Any] = Depends(common_query_params)
):
    query = """
        SELECT
            b.source,
            r.pays as region,
            COUNT(f.req_pk) as request_count
        FROM factrequests f
        JOIN dimbenchmarks b ON f.bench1_fk = b.benchmark_pk
        JOIN dimregions r ON f.region_fk = r.region_pk
    """
    
    filters = "WHERE 1=1"
    if params.get("start_date") and params.get("end_date"):
        filters += " AND f.pick_up_date::date BETWEEN :start_date AND :end_date"

    query = query + filters + " GROUP BY b.source, r.pays ORDER BY request_count DESC LIMIT 10"

    results = db.execute(text(query), params).fetchall()
    
    if not results:
        raise HTTPException(status_code=404, detail="Data not found")
    
    data = [
        schemas.BenchmarkBySourceAndRegionData(
            source=row.source.strip(), 
            region=row.region.strip(), 
            request_count=row.request_count
        )
        for row in results
    ]
    
    return schemas.RequestsPerBenchmarkBySourceAndRegion(data=data)


@app.get("/requests_per_offer/", response_model=schemas.RequestsPerOffer)
def get_requests_per_offer(
    db: Session = Depends(get_db),
    params: Dict[str, Any] = Depends(common_query_params)
):
    query = """
        SELECT
            o.adjustement_type,
            COUNT(f.req_pk) as request_count
        FROM factrequests f
        JOIN dimoffers o ON f.offer_fk = o.offer_pk
    """
    
    filters = "WHERE 1=1"
    if params.get("start_date") and params.get("end_date"):
        filters += " AND f.pick_up_date::date BETWEEN :start_date AND :end_date"

    query = query + filters + " GROUP BY o.adjustement_type"

    results = db.execute(text(query), params).fetchall()
    
    if not results:
        raise HTTPException(status_code=404, detail="Data not found")
    
    data = [
        schemas.OfferData(
            adjustement_type=row.adjustement_type.strip(), 
            request_count=row.request_count
        )
        for row in results
    ]
    
    return schemas.RequestsPerOffer(data=data)

@app.get("/clients_percentage_per_car_type_and_request_type/", response_model=schemas.ClientsPercentagePerCarTypeAndRequestType)
def get_clients_percentage_per_car_type_and_request_type(
    db: Session = Depends(get_db),
    params: Dict[str, Any] = Depends(common_query_params)
):
    query = """
        SELECT
            c.sub_type as car_type,
            r.req_type as request_type,
            COUNT(DISTINCT f.client_fk) as client_count
        FROM factrequests f
        JOIN dimcars c ON f.car_fk = c.car_pk
        JOIN dimrequesttypes r ON f.req_type_fk = r.req_type_pk
        JOIN dimregions AS dr ON f.region_fk = dr.region_pk
        JOIN dimdestinations AS dd ON dr.region_pk = dd.region_fk
    """
    
    filters = "WHERE 1=1"
    if params.get("slug"):
        filters += " AND c.slug = :slug"
    if params.get("start_date") and params.get("end_date"):
        filters += " AND f.pick_up_date::date BETWEEN :start_date AND :end_date"
    if params.get("dest_code"):
        filters += " AND dd.dest_code = :dest_code"

    query = query + filters + " GROUP BY c.sub_type, r.req_type"

    results = db.execute(text(query), params).fetchall()

    if not results:
        raise HTTPException(status_code=404, detail="Data not found")

    car_type_totals = {}
    for row in results:
        car_type = row.car_type
        if car_type in car_type_totals:
            car_type_totals[car_type] += row.client_count
        else:
            car_type_totals[car_type] = row.client_count

    data = [
        schemas.ClientsPercentageData(
            car_type=row.car_type.strip(),
            request_type=row.request_type.strip(),
            client_percentage=round((row.client_count / car_type_totals[row.car_type]) * 100, 2)
        )
        for row in results
        if row.car_type and row.request_type
    ]

    return schemas.ClientsPercentagePerCarTypeAndRequestType(data=data)




@app.get("/most_popular_requests/", response_model=schemas.MostPopularRequests)
def get_most_popular_requests(
    db: Session = Depends(get_db),
    params: Dict[str, Any] = Depends(common_query_params)
):
    query = """
        SELECT
            r.req_type as request_type,
            COUNT(f.req_pk) as request_count
        FROM factrequests f
        JOIN dimrequesttypes r ON f.req_type_fk = r.req_type_pk
        JOIN dimregions AS dr ON f.region_fk = dr.region_pk
        JOIN dimdestinations AS dd ON dr.region_pk = dd.region_fk
    """
    
    filters = "WHERE 1=1"
    if params.get("start_date") and params.get("end_date"):
        filters += " AND f.pick_up_date::date BETWEEN :start_date AND :end_date"

    if params.get("dest_code"):
        filters += " AND dd.dest_code = :dest_code"

    query = query + filters + " GROUP BY r.req_type ORDER BY request_count DESC LIMIT 10"

    results = db.execute(text(query), params).fetchall()
    
    if not results:
        raise HTTPException(status_code=404, detail="Data not found")
    
    data = [
        schemas.PopularRequestData(
            request_type=row.request_type.strip(), 
            request_count=row.request_count
        )
        for row in results
    ]
    
    return schemas.MostPopularRequests(data=data)

@app.get("/benchmark_performance_by_region/", response_model=schemas.BenchmarkPerformanceByRegion)
def get_benchmark_performance_by_region(
    db: Session = Depends(get_db),
    params: Dict[str, Any] = Depends(common_query_params)
):
    query = """
        SELECT
            b.source,
            r.pays as region,
            COUNT(f.req_pk) as request_count
        FROM factrequests f
        JOIN dimbenchmarks b ON f.bench1_fk = b.benchmark_pk
        JOIN dimregions r ON f.region_fk = r.region_pk
        JOIN dimdestinations AS dd ON r.region_pk = dd.region_fk
    """
    
    filters = "WHERE 1=1"
    if params.get("start_date") and params.get("end_date"):
        filters += " AND f.pick_up_date::date BETWEEN :start_date AND :end_date"

    if params.get("dest_code"):
        filters += " AND dd.dest_code = :dest_code"

    query = query + filters + " GROUP BY b.source, r.pays ORDER BY request_count DESC LIMIT 10"

    results = db.execute(text(query), params).fetchall()
    
    if not results:
        raise HTTPException(status_code=404, detail="Data not found")
    
    data = [
        schemas.BenchmarkPerformanceData(
            source=row.source.strip(), 
            region=row.region.strip(), 
            request_count=row.request_count
        )
        for row in results
    ]
    
    return schemas.BenchmarkPerformanceByRegion(data=data)


@app.get("/revenue_by_offer_and_date/", response_model=schemas.RevenueByOfferAndDate)
def get_revenue_by_offer_and_date(
    db: Session = Depends(get_db),
    params: Dict[str, Any] = Depends(common_query_params)
):
    query = """
        SELECT
            o.offer_code,
            d.date,
            SUM(f.total_price) as revenue
        FROM factrequests f
        JOIN dimoffers o ON f.offer_fk = o.offer_pk
        JOIN dimdates d ON f.date_fk = d.date_pk
        JOIN dimoffers AS doff ON f.offer_fk = doff.offer_pk
    """
    
    filters = "WHERE 1=1 "
    if params.get("start_date") and params.get("end_date"):
        filters += " AND f.pick_up_date::date BETWEEN :start_date AND :end_date"

    if params.get("adjustement_type"):
        filters += "AND doff.adjustement_type = :adjustement_type "

    query = query + filters + " GROUP BY o.offer_code, d.date , d.date LIMIT 5"

    results = db.execute(text(query), params).fetchall()
    
    if not results:
        raise HTTPException(status_code=404, detail="Data not found")
    
    data = [
        schemas.OfferRevenueData(
            offer_code=row.offer_code.strip(), 
            date=row.date.strftime('%Y-%m-%d'), 
            revenue=round(row.revenue,2)
        )
        for row in results
    ]
    
    return schemas.RevenueByOfferAndDate(data=data)


@app.get("/charge_kpi/", response_model=schemas.ChargeKPI)
def get_charge_kpi(
    db: Session = Depends(get_db),
    params: Dict[str, Any] = Depends(common_query_params)
):
    query = """
        SELECT SUM(f.prix_annuel) as total_charge
        FROM factrequests f
    """
    
    filters = "WHERE 1=1"
    if params.get("start_date") and params.get("end_date"):
        filters += " AND f.pick_up_date::date BETWEEN :start_date AND :end_date"

    result = db.execute(text(query + " " + filters), params).scalar()
    
    if result is None:
        raise HTTPException(status_code=404, detail="Data not found")
    
    return schemas.ChargeKPI(total_charge=round(result,2))



@app.get("/most_revenue_generating_countries/", response_model=schemas.MostRevenueGeneratingCountries)
def get_most_revenue_generating_countries(
    db: Session = Depends(get_db),
    params: Dict[str, Any] = Depends(common_query_params)
):
    query = """
        SELECT
            r.pays as country,
            SUM(f.total_price) as total_revenue
        FROM factrequests f
        JOIN dimregions r ON f.region_fk = r.region_pk
        JOIN dimdestinations AS dd ON r.region_pk = dd.region_fk
        
    """
    
    filters = "WHERE 1=1"
    if params.get("start_date") and params.get("end_date"):
        filters += " AND f.pick_up_date::date BETWEEN :start_date AND :end_date"

    
    if params.get("dest_code"):
        filters += " AND dd.dest_code = :dest_code"

    query = query + filters + " GROUP BY r.pays ORDER BY total_revenue DESC "

    results = db.execute(text(query), params).fetchall()
    
    if not results:
        raise HTTPException(status_code=404, detail="Data not found")
    
    data = [
        schemas.RevenueGeneratingCountry(
            country=row.country.strip(), 
            total_revenue=row.total_revenue
        )
        for row in results
    ]
    
    return schemas.MostRevenueGeneratingCountries(data=data)


@app.get("/revenue_over_time_per_benchmark/", response_model=schemas.RevenueOverTimeResponse)
def get_revenue_over_time_per_benchmark(
    db: Session = Depends(get_db),
    params: Dict[str, Any] = Depends(common_query_params)
):
    query = """
        SELECT
            d.date,
            b.source,
            SUM(f.total_price) as total_revenue
        FROM factrequests f
        JOIN dimdates d ON f.date_fk = d.date_pk
        JOIN dimbenchmarks b ON f.bench1_fk = b.benchmark_pk
    """
    
    filters = "WHERE 1=1"
    if params.get("start_date") and params.get("end_date"):
        filters += " AND f.pick_up_date::date BETWEEN :start_date AND :end_date"

    query = query + filters + " GROUP BY d.date, b.source ORDER BY total_revenue DESC LIMIT 20"

    results = db.execute(text(query), params).fetchall()
    
    if not results:
        raise HTTPException(status_code=404, detail="Data not found")
    
    data = [
        schemas.RevenueOverTimeData(
            date=row.date.strftime('%Y-%m-%d'), 
            source=row.source.strip(), 
            total_revenue=row.total_revenue
        )
        for row in results
    ]
    
    return schemas.RevenueOverTimeResponse(data=data)


@app.get("/timeline_kpi/", response_model=schemas.TimelineKPIResponse)
def get_timeline_kpi(
    db: Session = Depends(get_db),
    params: Dict[str, Any] = Depends(common_query_params)
):
    query = """
        SELECT
            d.date,
            COUNT(f.req_pk) as request_count,
            SUM(f.total_price) as total_revenue
        FROM factrequests f
        JOIN dimdates d ON f.date_fk = d.date_pk
    """
    
    filters = "WHERE 1=1"
    if params.get("start_date") and params.get("end_date"):
        filters += " AND f.pick_up_date::date BETWEEN :start_date AND :end_date"

    query = query + filters + " GROUP BY d.date ORDER BY d.date"

    results = db.execute(text(query), params).fetchall()
    
    if not results:
        raise HTTPException(status_code=404, detail="Data not found")
    
    data = [
        schemas.TimelineKPIData(
            date=row.date.strftime('%Y-%m-%d'), 
            request_count=row.request_count, 
            total_revenue=row.total_revenue
        )
        for row in results
    ]
    
    return schemas.TimelineKPIResponse(data=data)






@app.get("/quarterly_revenue_per_request_type/", response_model=schemas.QuarterlyRevenueResponse)
def get_quarterly_revenue_per_request_type(
    db: Session = Depends(get_db),
    params: Dict[str, Any] = Depends(common_query_params)
):
    query = """
        SELECT
            CONCAT(d."Annee", ' Q', d.trimestre) as quarter,
            rt.req_type as request_type,
            SUM(f.total_price) as total_revenue
        FROM factrequests f
        JOIN dimdates d ON f.date_fk = d.date_pk
        JOIN dimrequesttypes rt ON f.req_type_fk = rt.req_type_pk
    """
    
    filters = "WHERE 1=1 "
    if params.get("start_date") and params.get("end_date"):
        filters += " AND f.pick_up_date::date BETWEEN :start_date AND :end_date"

    if params.get("req_type"):
        filters += "AND rt.req_type = :req_type "

    query = query + filters + " GROUP BY CONCAT(d.\"Annee\", ' Q', d.trimestre), rt.req_type ORDER BY quarter"

    results = db.execute(text(query), params).fetchall()
    
    if not results:
        raise HTTPException(status_code=404, detail="Data not found")
    
    data = [
        schemas.QuarterlyRevenueData(
            quarter=row.quarter, 
            request_type=row.request_type.strip(), 
            total_revenue=row.total_revenue
        )
        for row in results
    ]
    
    return schemas.QuarterlyRevenueResponse(data=data)

@app.get("/total_amount_and_percentage_gain_per_month/", response_model=schemas.MonthlyGainResponse)
def get_total_amount_and_percentage_gain_per_month(
    db: Session = Depends(get_db),
    params: Dict[str, Any] = Depends(common_query_params)
):
    query = """
        WITH monthly_data AS (
            SELECT
                d."Annee",
                d.lib_mois as month,
                d.id_mois,
                SUM(f.total_price) as total_amount
            FROM factrequests f
            JOIN dimdates d ON f.date_fk = d.date_pk
            GROUP BY d."Annee", d.lib_mois, d.id_mois
            ORDER BY d."Annee", d.id_mois
        )
        SELECT
            md."Annee",
            md.month,
            md.total_amount,
            (md.total_amount - COALESCE(lag(md.total_amount) OVER (PARTITION BY md."Annee" ORDER BY md.id_mois), 0)) / NULLIF(md.total_amount, 0) * 100 as percentage_gain
        FROM monthly_data md
    """

    results = db.execute(text(query), params).fetchall()
    
    if not results:
        raise HTTPException(status_code=404, detail="Data not found")
    
    data = [
        schemas.MonthlyGainData(
            year=row.Annee, 
            month=row.month.strip(), 
            total_amount=round(row.total_amount,2), 
            percentage_gain=round(row.percentage_gain,2)
        )
        for row in results
    ]
    
    return schemas.MonthlyGainResponse(data=data)

@app.get("/car_profitability_kpi/", response_model=schemas.CarProfitabilityResponse)
def get_car_profitability_kpi(
    db: Session = Depends(get_db),
    slug: List[str] = Query(None),
    start_date: str = Query(None),
    end_date: str = Query(None)
):
    query = """
        SELECT
            c.brand || ' ' || c.plate_number as car_model,
            COALESCE(SUM(f.total_price), 0) as total_revenue,
            COALESCE(SUM(f.prix_annuel), 0) as prix_annuel,
            COALESCE(SUM(f.total_price) - SUM(f.prix_annuel), 0) as profitability
        FROM factrequests f
        JOIN dimcars c ON f.car_fk = c.car_pk
    """
    
    filters = "WHERE 1=1 "
    params= {}
    if slug:
        filters += " AND c.slug IN :slug"
        params['slug'] = tuple(slug)  # Convert list to tuple for SQL query
    
    if start_date and end_date:
        filters += " AND f.pick_up_date::date BETWEEN :start_date AND :end_date"
        params['start_date'] = start_date
        params['end_date'] = end_date

    query = query + filters + " GROUP BY car_model ORDER BY profitability DESC"

    results = db.execute(text(query), params).fetchall()
    
    if not results:
        raise HTTPException(status_code=404, detail="Data not found")
    
    data = [
        schemas.CarProfitabilityData(
            car_model=row.car_model, 
            total_revenue=row.total_revenue, 
            prix_annuel=row.prix_annuel, 
            profitability=row.profitability
        )
        for row in results
    ]
    
    return schemas.CarProfitabilityResponse(data=data)

@app.get("/profit_total_and_other_charges/", response_model=schemas.ProfitChargesResponse)
def get_profit_total_and_other_charges(
    db: Session = Depends(get_db),
    params: Dict[str, Any] = Depends(common_query_params)
):
    query = """
        SELECT
            d."Annee" || '-' || d.lib_mois as month,
            COALESCE(SUM(f.total_price), 0) as total_revenue,
            COALESCE(SUM(f.prix_annuel), 0) as other_charges,
            COALESCE(SUM(f.total_price) - SUM(f.prix_annuel), 0) as total_profit
        FROM factrequests f
        JOIN dimdates d ON f.date_fk = d.date_pk
    """
    
    filters = "WHERE 1=1"
    if params.get("start_date") and params.get("end_date"):
        filters += " AND f.pick_up_date::date BETWEEN :start_date AND :end_date"

    query = query + filters + " GROUP BY d.\"Annee\", d.lib_mois, d.id_mois ORDER BY d.\"Annee\", d.id_mois"

    results = db.execute(text(query), params).fetchall()
    
    if not results:
        raise HTTPException(status_code=404, detail="Data not found")
    
    data = [
        schemas.ProfitChargesData(
            month=row.month, 
            total_revenue=row.total_revenue, 
            other_charges=row.other_charges, 
            total_profit=row.total_profit
        )
        for row in results
    ]
    
    return schemas.ProfitChargesResponse(data=data)

@app.get("/car_rentation_rate_overtime", response_model=schemas.CarRentationRateKPI)
def get_car_rentation_rate_overtime(db: Session = Depends(get_db)):
    quarters_query = text("""
        SELECT DISTINCT DATE_TRUNC('quarter', pick_up_date) AS quarter
        FROM factrequests
        WHERE pick_up_date <= CURRENT_DATE
        ORDER BY quarter DESC
        LIMIT 8
    """)

    quarters = db.execute(quarters_query).fetchall()
    
    if not quarters:
        raise HTTPException(status_code=404, detail="No data available")

    rentation_rates = []
    
    for i in range(len(quarters) - 1):
        current_quarter = quarters[i][0]
        previous_quarter = quarters[i + 1][0]

        current_car_query = text(f"""
            SELECT DISTINCT car_fk
            FROM factrequests
            WHERE DATE_TRUNC('quarter', pick_up_date) = '{current_quarter}'
        """)
        
        previous_car_query = text(f"""
            SELECT DISTINCT car_fk
            FROM factrequests
            WHERE DATE_TRUNC('quarter', pick_up_date) = '{previous_quarter}'
        """)
        
        current_car = db.execute(current_car_query).fetchall()
        previous_car = db.execute(previous_car_query).fetchall()

        current_car_set = set([row[0] for row in current_car])
        previous_car_set = set([row[0] for row in previous_car])
        
        if not previous_car_set:
            continue

        new_car_set = current_car_set - previous_car_set
        retained_car_set = current_car_set & previous_car_set


        retained_car_count = len(retained_car_set)
        previous_car_count = len(previous_car_set)

        if previous_car_count == 0:
            rentation_rate = 0
        else:
            rentation_rate = retained_car_count / previous_car_count


        year = current_quarter.year
        quarter = (current_quarter.month - 1) // 3 + 1
        quarter_str = f"{year}-Q{quarter}"

        rentation_rates.append(schemas.CarRentationRatePoint(quarter=quarter_str, rate=round(rentation_rate,2)))

    if not rentation_rates:
        raise HTTPException(status_code=404, detail="Not enough data to calculate rentation rates")

    rentation_data = schemas.CarRentationRateData(data=rentation_rates)
    return schemas.CarRentationRateKPI(data=rentation_data)

@app.get("/top_pickup_places/", response_model=schemas.TopPlacesResponse)
def get_top_pickup_places(
    db: Session = Depends(get_db),
    params: Dict[str, Any] = Depends(common_query_params)
):
    query = """
        SELECT
            TRIM(f.pick_up_place) AS place,
            COUNT(*) AS request_count
        FROM factrequests f
        JOIN dimregions AS dr ON f.region_fk = dr.region_pk
        JOIN dimdestinations AS dd ON dr.region_pk = dd.region_fk
    """
    
    filters = "WHERE 1=1"
    if params.get("dest_code"):
        filters += " AND dd.dest_code = :dest_code"
    if params.get("start_date") and params.get("end_date"):
        filters += " AND f.pick_up_date::date BETWEEN :start_date AND :end_date"
    
    query = query + filters + " GROUP BY f.pick_up_place ORDER BY request_count DESC LIMIT 10"
    
    results = db.execute(text(query), params).fetchall()
    
    if not results:
        raise HTTPException(status_code=404, detail="No data found")
    
    data = [
        schemas.TopPlace(
            place=row.place.strip(), 
            request_count=row.request_count
        )
        for row in results
    ]
    
    return schemas.TopPlacesResponse(data=data)

@app.get("/top_dropoff_places/", response_model=schemas.TopPlacesResponse)
def get_top_dropoff_places(
    db: Session = Depends(get_db),
    params: Dict[str, Any] = Depends(common_query_params)
):
    query = """
        SELECT
            TRIM(f.drop_off_place) AS place,
            COUNT(*) AS request_count
        FROM factrequests f
        JOIN dimregions AS dr ON f.region_fk = dr.region_pk
        JOIN dimdestinations AS dd ON dr.region_pk = dd.region_fk
    """
    
    filters = "WHERE 1=1"
    if params.get("dest_code"):
        filters += " AND dd.dest_code = :dest_code"
    if params.get("start_date") and params.get("end_date"):
        filters += " AND f.pick_up_date::date BETWEEN :start_date AND :end_date"
    
    query = query + filters + " GROUP BY f.drop_off_place ORDER BY request_count DESC LIMIT 10"
    
    results = db.execute(text(query), params).fetchall()
    
    if not results:
        raise HTTPException(status_code=404, detail="No data found")
    
    data = [
        schemas.TopPlace(
            place=row.place.strip() if row.place else "Unknown", 
            request_count=row.request_count
        )
        for row in results
    ]
    
    return schemas.TopPlacesResponse(data=data)

@app.get("/monthly_revenue_and_gain/", response_model=schemas.MonthlyRevenueAndGainResponse)
def get_monthly_revenue_and_gain(
    db: Session = Depends(get_db)
):
    query = """
        WITH monthly_revenue AS (
            SELECT
                d."Annee" AS year,
                d.id_mois AS month_id,
                d.lib_mois AS month_name,
                COALESCE(SUM(f.total_price), 0) as total_revenue
            FROM factrequests f
            JOIN dimdates d ON f.date_fk = d.date_pk
            GROUP BY d."Annee", d.id_mois, d.lib_mois
            ORDER BY d."Annee" DESC, d.id_mois DESC
            LIMIT 2
        )
        SELECT * FROM monthly_revenue ORDER BY year DESC, month_id DESC;
    """

    results = db.execute(text(query)).fetchall()
    
    if not results or len(results) < 2:
        raise HTTPException(status_code=404, detail="Not enough data to calculate the comparison")

    last_month = results[0]
    second_last_month = results[1]

    if second_last_month.total_revenue != 0:
        percentage_gain = ((last_month.total_revenue - second_last_month.total_revenue) / second_last_month.total_revenue) * 100
    else:
        percentage_gain = 0

    return schemas.MonthlyRevenueAndGainResponse(
        last_month_total_revenue=round(last_month.total_revenue, 2),
        percentage_gain=round(percentage_gain, 2)
    )

@app.get("/car/image", response_model=schemas.ImageKPI)
def get_car_image(
    db: Session = Depends(get_db),
    params: Dict[str, Any] = Depends(common_query_params)
):
    query = """
        SELECT 
            c.image
        FROM dimcars c
        JOIN factrequests f ON c.car_pk = f.car_fk
        JOIN dimdates d ON f.date_fk = d.date_pk
    """
    
    filters = "WHERE 1=1"
    if params.get("start_date") and params.get("end_date"):
        filters += " AND d.date BETWEEN :start_date AND :end_date"
    if params.get("slug"):
        filters += " AND c.slug = :slug"
    
    query = query + filters + " LIMIT 1"
    
    result = db.execute(text(query), params).fetchone()
    
    if not result:
        raise HTTPException(status_code=404, detail="Image not found")
    
    return schemas.ImageKPI(image_url=result.image)

@app.get("/total_price/", response_model=schemas.TotalPriceResponse)
def get_total_price(
    db: Session = Depends(get_db),
    params: Dict[str, Any] = Depends(common_query_params)
):
    query = """
        SELECT
            SUM(f.total_price) as total_price
        FROM factrequests f
        JOIN dimdates d ON f.date_fk = d.date_pk
    """
    
    filters = "WHERE 1=1"
    if params.get("start_date") and params.get("end_date"):
        filters += " AND d.date BETWEEN :start_date AND :end_date"
    
    query = query + filters

    result = db.execute(text(query), params).scalar()
    
    if result is None:
        raise HTTPException(status_code=404, detail="No data found")
    
    return schemas.TotalPriceResponse(total_price=round(result,2))


@app.get("/filters/car-types", response_model=List[str])
async def get_car_types(db: Session = Depends(get_db)):
    query = 'select slug from dimcars'
    results = db.execute(text(query)).fetchall()  # Results are tuples
    return [result[0].strip() for result in results]  # Access by index


@app.get("/filters/dates", response_model=List[str])
async def get_dates(db: Session = Depends(get_db)):
    query = 'select date from dimdates'
    results = db.execute(text(query)).fetchall()  # Returns tuples
    return [result[0].strftime('%Y-%m-%d') for result in results]  # Access by index


@app.get("/filters/dests", response_model=List[str])
async def get_dests(db: Session = Depends(get_db)):
    query = 'select dest_code from dimdestinations'
    results = db.execute(text(query)).fetchall()  # Returns tuples
    return [result[0].strip() for result in results]  # Access by index


@app.get("/predict_total_price/", response_model=schemas.TotalPricePrediction)
def predict_total_price(future_date: str, db: Session = Depends(get_db)):
    query = """
    SELECT dimdates.date, factrequests.total_price 
    FROM factrequests
    JOIN dimdates ON factrequests.date_fk = dimdates.date_pk
    """
    result = db.execute(text(query))
    df = pd.DataFrame(result.fetchall(), columns=['date', 'total_price'])

    df['date'] = pd.to_datetime(df['date'])
    df['date_numeric'] = (df['date'] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1D')

    X = df[['date_numeric']]
    y = df['total_price']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)

    model = LinearRegression()
    model.fit(X_train, y_train)

    try:
        future_date_obj = datetime.strptime(future_date, '%Y-%m-%d')
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

    future_date_numeric = (future_date_obj - pd.Timestamp("1970-01-01")) // pd.Timedelta('1D')

    predicted_price = model.predict([[future_date_numeric]])
    return schemas.TotalPricePrediction(predicted_total_price=round(predicted_price[0], 2))

@app.get("/optimize_fleet/", response_model=schemas.FleetOptimization)
def optimize_fleet(future_date: str, db: Session = Depends(get_db)):
    query = """
    SELECT dimdates.date, factrequests.total_price, factrequests.passenger_count_client 
    FROM factrequests
    JOIN dimdates ON factrequests.date_fk = dimdates.date_pk
    """
    result = db.execute(text(query))
    df = pd.DataFrame(result.fetchall(), columns=['date', 'total_price', 'passenger_count_client'])

    df['date'] = pd.to_datetime(df['date'])
    df['date_numeric'] = (df['date'] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1D')

    X = df[['date_numeric']]
    y = df['passenger_count_client']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)

    model = LinearRegression()
    model.fit(X_train, y_train)

    try:
        future_date_obj = datetime.strptime(future_date, '%Y-%m-%d')
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

    future_date_numeric = (future_date_obj - pd.Timestamp("1970-01-01")) // pd.Timedelta('1D')

    predicted_passenger_count = math.ceil(model.predict([[future_date_numeric]])[0])

    vehicle_capacity = 4
    recommended_fleet_size = math.ceil(predicted_passenger_count / vehicle_capacity)
    recommended_fleet_size_adjusted = math.ceil(recommended_fleet_size / 0.85)

    return schemas.FleetOptimization(
        predicted_passenger_count=predicted_passenger_count,
        recommended_fleet_size=recommended_fleet_size,
        adjusted_fleet_size=recommended_fleet_size_adjusted
    )