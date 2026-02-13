with source as (
    select *
    from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select
        dispatching_base_num,
        cast(PUlocationID as integer) as pickup_location_id,
        cast(DOlocationID as integer) as dropoff_location_id,
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,
        cast(passenger_count as integer) as passenger_count,
        cast(trip_distance as numeric) as trip_distance,
        cast(fare_amount as numeric) as fare_amount,
        cast(extra as numeric) as extra,
        cast(mta_tax as numeric) as mta_tax,
        cast(tip_amount as numeric) as tip_amount,
        cast(tolls_amount as numeric) as tolls_amount,
        cast(improvement_surcharge as numeric) as improvement_surcharge,
        cast(total_amount as numeric) as total_amount
    from source
    where dispatching_base_num is not null
)

select * from renamed
