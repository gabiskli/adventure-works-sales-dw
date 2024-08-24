{{
    config(
        materialized = "table"
    )
}}

with raw_dates as (
    {{ dbt_date.get_date_dimension("2011-01-01", "2014-12-31") }}
    )
    , selected_columns as (
        select
            date_day as pk_date
            , format_datetime('%Y-%m', date_day) as year_and_month
            , day_of_week_iso as day_of_week_number --Monday = 1 and Sunday = 7
            , day_of_week_name as day_of_week
            , day_of_week_name_short as day_of_week_short
            , day_of_month as day_of_month
            , day_of_year as day_of_year 
            , iso_week_start_date as week_start_date
            , iso_week_end_date as week_end_sate
            , month_of_year as month_of_year
            , month_name as month_name
            , month_name_short as month_name_short
            , year_number as year_number 
            , quarter_of_year as quarter
            --, DAY_OF_WEEK
            --, WEEK_START_DATE
            --, WEEK_END_DATE
            --, WEEK_OF_YEAR
            --, ISO_WEEK_OF_YEAR
            --, MONTH_START_DATE
            --, MONTH_END_DATE
            --, QUARTER_START_DATE
            --, QUARTER_END_DATE
            --PRIOR_DATE_DAY
            --NEXT_DATE_DAY
            --PRIOR_YEAR_DATE_DAY
            --PRIOR_YEAR_OVER_YEAR_DATE_DAY
            --PRIOR_YEAR_WEEK_START_DATE
            --PRIOR_YEAR_WEEK_END_DATE
            --prior_year_iso_week_start_date
            --PRIOR_YEAR_WEEK_OF_YEAR
            --PRIOR_YEAR_ISO_WEEK_OF_YEAR
            --PRIOR_YEAR_MONTH_START_DATE
            --PRIOR_YEAR_MONTH_END_DATE
            --YEAR_START_DATE
            --YEAR_END_DATE
        from raw_dates
    )
select *
from selected_columns