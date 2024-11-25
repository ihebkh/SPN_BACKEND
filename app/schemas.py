from pydantic import BaseModel
from typing import List, Optional
from datetime import date


class FactRequestGainData(BaseModel):
    gain: float
    customer_name:str

class FactRequestGain(BaseModel):
    data: List[FactRequestGainData]



class BrandGain(BaseModel):
    brand: str
    total_gain: float

class BrandGainsKPI(BaseModel):
    data: List[BrandGain]


class CarPlateGain(BaseModel):
    plate_number: str
    gain: float
    brand:str

class CarPlateGainsKPI(BaseModel):
    data: List[CarPlateGain]


class CarClientsData(BaseModel):
    brand: str
    req_type: str
    client_count: int

class CarClientsKPI(BaseModel):
    data: List[CarClientsData]

class TotalGain(BaseModel):
    total_gain: float   

class CarOwnersData(BaseModel):
    owner: str
    car_count: int
    request_percentage: float

class CarOwnersKPI(BaseModel):
    data: List[CarOwnersData]

class CarRetentionRate(BaseModel):
    retention_rate: float

class BenchmarkBySourceAndRegionData(BaseModel):
    source: str
    region: str
    request_count: int

class RequestsPerBenchmarkBySourceAndRegion(BaseModel):
    data: List[BenchmarkBySourceAndRegionData]

class OfferPerformanceData(BaseModel):
    offer_code: str
    region: str
    request_count: int

class OfferPerformanceByRegion(BaseModel):
    data: List[OfferPerformanceData]

class OfferData(BaseModel):
    adjustement_type: str
    request_count: int

class RequestsPerOffer(BaseModel):
    data: List[OfferData]

class ClientsPercentageData(BaseModel):
    car_type: str
    request_type: str
    client_percentage: float

class ClientsPercentagePerCarTypeAndRequestType(BaseModel):
    data: List[ClientsPercentageData]

class PopularRequestData(BaseModel):
    request_type: str
    request_count: int

class MostPopularRequests(BaseModel):
    data: List[PopularRequestData]

class BenchmarkPerformanceData(BaseModel):
    source: str
    region: str
    request_count: int

class BenchmarkPerformanceByRegion(BaseModel):
    data: List[BenchmarkPerformanceData]

class OfferRevenueData(BaseModel):
    offer_code: str
    date: date
    revenue: float

class RevenueByOfferAndDate(BaseModel):
    data: List[OfferRevenueData]

class ChargeKPI(BaseModel):
    total_charge: float

class RevenueGeneratingCountry(BaseModel):
    country: str
    total_revenue: float

class MostRevenueGeneratingCountries(BaseModel):
    data: List[RevenueGeneratingCountry]

class OfferClientRevenueData(BaseModel):
    offer_code: str
    customer: str
    total_revenue: float

class TotalRevenuePerOfferClient(BaseModel):
    data: List[OfferClientRevenueData]

class RevenueOverTimeData(BaseModel):
    date: str
    source: str
    total_revenue: float

class RevenueOverTimeResponse(BaseModel):
    data: List[RevenueOverTimeData]

class TimelineKPIData(BaseModel):
    date: str
    request_count: int
    total_revenue: float

class TimelineKPIResponse(BaseModel):
    data: List[TimelineKPIData]

class QuarterlyRevenueData(BaseModel):
    quarter: str
    request_type: str
    total_revenue: float

class QuarterlyRevenueResponse(BaseModel):
    data: List[QuarterlyRevenueData]

class MonthlyGainData(BaseModel):
    year: int
    month: str
    total_amount: float
    percentage_gain: float

class MonthlyGainResponse(BaseModel):
    data: List[MonthlyGainData]

class CarProfitabilityData(BaseModel):
    car_model: str
    total_revenue: float
    prix_annuel: float
    profitability: float

class CarProfitabilityResponse(BaseModel):
    data: List[CarProfitabilityData]

class ProfitChargesData(BaseModel):
    month: str
    total_revenue: float
    other_charges: float
    total_profit: float

class ProfitChargesResponse(BaseModel):
    data: List[ProfitChargesData]









class BenchmarkData(BaseModel):
    benchmark_code: str
    request_count: int






class brandGain(BaseModel):
    brand: str
    total_gain: float

class CarPlateGain(BaseModel):
    plate_number: str
    gain: float




class SourceDetail(BaseModel):
    source: str
    offer_price: float

    class Config:
        from_attributes = True

class BenchmarkDetail(BaseModel):
    benchmark_code: str
    sources: List[SourceDetail]

    class Config:
        from_attributes = True

class RequestBenchmark(BaseModel):
    req_code: str
    benchmarks: List[BenchmarkDetail]

    class Config:
        from_attributes = True

class RequestBenchmarkSourcesResponse(BaseModel):
    requests: List[RequestBenchmark]

    class Config:
        from_attributes = True    




class RequestGain(BaseModel):
    request_id: int
    gain: float

class GrowthKPI(BaseModel):
    gain_2022: float
    gain_2023: float
    gain_2024: float
    growth_2023: float
    growth_2024: float

class CarRentationRatePoint(BaseModel):
    quarter: str
    rate: float

class CarRentationRateData(BaseModel):
    data: List[CarRentationRatePoint]

class CarRentationRateKPI(BaseModel):
    data: CarRentationRateData

class RentalDuration(BaseModel):
    request_id: int
    pick_up_date: Optional[str]
    drop_off_date: Optional[str]
    rental_duration: Optional[int]

class TopPlace(BaseModel):
    place: str
    request_count: int

class TopPlacesResponse(BaseModel):
    data: List[TopPlace]


class MonthlyRevenueAndGainResponse(BaseModel):
    last_month_total_revenue: float
    percentage_gain: float


class ImageKPI(BaseModel):
    image_url: str

class TotalPriceResponse(BaseModel):
    total_price: float

class TotalPricePrediction(BaseModel):
    predicted_total_price: float

class FleetOptimization(BaseModel):
    predicted_passenger_count: int
    recommended_fleet_size: int
    adjusted_fleet_size: int