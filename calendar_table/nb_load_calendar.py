#!/usr/bin/env python
# coding: utf-8

# ## nb_load_calendar
# 
# New notebook

# In[ ]:


from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from typing import List

# Function to generate calendar date table
def generate_calendar_dataframe(start_date: str = "1900-01-01", end_date: str = "2999-12-31") -> DataFrame:
    """
    Generates a calendar DataFrame with enriched date attributes between a specified start and end date.

    :param spark: SparkSession object used to create the DataFrame.
    :param start_date: The start date for the calendar, default is "1901-01-01".
    :param end_date: The end date for the calendar, default is "2999-12-31".

    :return: DataFrame with calendar dates and their associated attributes.

    This function performs the following steps:
    1. Creates a DataFrame with a sequence of dates from start_date to end_date.
    2. Enriches the DataFrame with various date-related attributes such as day of week, week of year, month, quarter, year, etc.
    """
    # Create a DataFrame with the date range difference in days
    date_diff = spark.createDataFrame([(start_date, end_date)], ["start", "end"])
    date_diff = date_diff.withColumn("diff", datediff(col("end"), col("start"))).collect()[0]["diff"]

    # Generate a DataFrame with the sequence of dates
    date_range_diff = spark.range(0, date_diff + 1).withColumnRenamed("id", "day_id")
    start_date_df = spark.createDataFrame([(start_date,)], ["start_date"])

    # Cast 'day_id' to integer and create a sequence of dates by adding it to 'start_date'
    df_date = start_date_df.crossJoin(date_range_diff) \
        .select(date_add(col("start_date"), col("day_id").cast("int")).alias("date"))

    # Enrich the DataFrame with date attributes
    df_stage = df_date.select(
        date_format(col("date"), "yyyyMMdd").cast("int").alias("calendar_date_key"),
        
        # Weekday
        col("date").alias("calendar_date"),
        dayofweek(col("date")).cast("int").alias("calendar_weekday_number"),
        date_format(col("date"), 'EEEE').alias("calendar_weekday"),
        
        # Week
        lpad((weekofyear(col("date")) + when(dayofweek(col("date")) == 1, 1).otherwise(0)).cast("string"), 2, '0').alias("calendar_week_number"),
        date_sub(col("date"), when(dayofweek(col("date")) == 1, 0).otherwise(dayofweek(col("date")) - 1)).alias("calendar_start_of_week"),
        date_add(col("date"), when(dayofweek(col("date")) == 1, 6).otherwise(7 - dayofweek(col("date")))).alias("calendar_end_of_week"),
        concat(year(col("date")), lpad((weekofyear(col("date")) + when(dayofweek(col("date")) == 1, 1).otherwise(0)).cast("string"), 2, '0')).alias("calendar_year_week_number"),
        concat(lit('Week '), lpad((weekofyear(col("date")) + when(dayofweek(col("date")) == 1, 1).otherwise(0)).cast("string"), 2, '0'), lit(', '), year(col("date"))).alias("calendar_week_year"),

        # Previous Week
        lpad((weekofyear(date_sub(col("date"), 7)) + when(dayofweek(date_sub(col("date"), 7)) == 1, 1).otherwise(0)).cast("string"), 2, '0').alias("previous_week_calendar_week_number"),
        date_sub(date_sub(col("date"), 7), when(dayofweek(date_sub(col("date"), 7)) == 1, 0).otherwise(dayofweek(date_sub(col("date"), 7)) - 1)).alias("previous_week_calendar_start_of_week"),
        date_add(date_sub(col("date"), 7), when(dayofweek(date_sub(col("date"), 7)) == 1, 6).otherwise(7 - dayofweek(date_sub(col("date"), 7)))).alias("previous_week_calendar_end_of_week"),
        concat(year(date_sub(col("date"), 7)), lpad((weekofyear(date_sub(col("date"), 7)) + when(dayofweek(date_sub(col("date"), 7)) == 1, 1).otherwise(0)).cast("string"), 2, '0')).alias("previous_week_calendar_year_week_number"),
        concat(lit('Week '), lpad((weekofyear(date_sub(col("date"), 7)) + when(dayofweek(date_sub(col("date"), 7)) == 1, 1).otherwise(0)).cast("string"), 2, '0'), lit(', '), year(date_sub(col("date"), 7))).alias("previous_week_calendar_week_year"),

        # Same Week Previous Year
        lpad((weekofyear(date_sub(col("date"), 364)) + when(dayofweek(date_sub(col("date"), 364)) == 1, 1).otherwise(0)).cast("string"), 2, '0').alias("previous_year_calendar_week_number"),
        date_sub(date_sub(col("date"), 364), when(dayofweek(date_sub(col("date"), 364)) == 1, 0).otherwise(dayofweek(date_sub(col("date"), 364)) - 1)).alias("previous_year_calendar_start_of_week"),
        date_add(date_sub(col("date"), 364), when(dayofweek(date_sub(col("date"), 364)) == 1, 6).otherwise(7 - dayofweek(date_sub(col("date"), 364)))).alias("previous_year_calendar_end_of_week"),
        concat(year(date_sub(col("date"), 364)), lpad((weekofyear(date_sub(col("date"), 364)) + when(dayofweek(date_sub(col("date"), 364)) == 1, 1).otherwise(0)).cast("string"), 2, '0')).alias("previous_year_calendar_year_week_number"),
        concat(lit('Week '), lpad((weekofyear(date_sub(col("date"), 364)) + when(dayofweek(date_sub(col("date"), 364)) == 1, 1).otherwise(0)).cast("string"), 2, '0'), lit(', '), year(date_sub(col("date"), 364))).alias("previous_year_calendar_week_year"),
        
        # Month
        lpad(month(col("date")).cast("string"), 2, '0').alias("calendar_month_number"),
        date_format(col("date"), 'MMMM').alias("calendar_month"),
        dayofmonth(col("date")).alias("calendar_day_of_month"),
        trunc(col("date"), 'month').alias("calendar_start_of_month"),
        last_day(col("date")).alias("calendar_end_of_month"),
        concat(year(col("date")), lpad(month(col("date")).cast("string"), 2, '0')).alias("calendar_year_month_number"),
        date_format(col("date"), 'MMMM yyyy').alias("calendar_month_year"),
        dayofmonth(last_day(col("date"))).alias("calendar_month_days"),

        # Previous Month
        lpad(month(add_months(col("date"), -1)).cast("string"), 2, '0').alias("previous_month_calendar_month_number"),
        date_format(add_months(col("date"), -1), 'MMMM').alias("previous_month_calendar_month"),
        dayofmonth(add_months(col("date"), -1)).alias("previous_month_calendar_day_of_month"),
        trunc(add_months(col("date"), -1), 'month').alias("previous_month_calendar_start_of_month"),
        last_day(add_months(col("date"), -1)).alias("previous_month_calendar_end_of_month"),
        concat(year(add_months(col("date"), -1)), lpad(month(add_months(col("date"), -1)).cast("string"), 2, '0')).alias("previous_month_calendar_year_month_number"),
        date_format(add_months(col("date"), -1), 'MMMM yyyy').alias("previous_month_calendar_month_year"),
        dayofmonth(last_day(add_months(col("date"), -1))).alias("previous_month_calendar_month_days"),
        
        # Previous Year Month
        lpad(month(add_months(col("date"), -12)).cast("string"), 2, '0').alias("previous_year_calendar_month_number"),
        date_format(add_months(col("date"), -12), 'MMMM').alias("previous_year_calendar_month"),
        dayofmonth(add_months(col("date"), -12)).alias("previous_year_calendar_day_of_month"),
        trunc(add_months(col("date"), -12), 'month').alias("previous_year_calendar_start_of_month"),
        last_day(add_months(col("date"), -12)).alias("previous_year_calendar_end_of_month"),
        concat(year(add_months(col("date"), -12)), lpad(month(add_months(col("date"), -12)).cast("string"), 2, '0')).alias("previous_year_calendar_year_month_number"),
        date_format(add_months(col("date"), -12), 'MMMM yyyy').alias("previous_year_calendar_month_year"),
        dayofmonth(last_day(add_months(col("date"), -12))).alias("previous_year_calendar_month_days"),

        # Quarter
        quarter(col("date")).cast("string").alias("calendar_quarter_number"),
        concat(lit('Q'), quarter(col("date")).cast("string")).alias("calendar_quarter"),
        (datediff(col("date"), date_trunc('quarter', col("date"))) + 1).alias("calendar_day_of_quarter"),
        date_format(date_trunc('quarter', col("date")), 'yyyy-MM-dd').alias("calendar_start_of_quarter"),
        last_day(add_months(expr("date_trunc('quarter', date)"), 2)).alias("calendar_end_of_quarter"),
        concat(year(col("date")), lpad(quarter(col("date")).cast("string"), 2, '0')).alias("calendar_year_quarter_number"),
        concat(lit('Q'), quarter(col("date")).cast("string"), lit(' '), year(col("date"))).alias("calendar_quarter_year"),
        expr("datediff(last_day(add_months(date_trunc('quarter', date), 2)), date_trunc('quarter', date)) + 1").alias("calendar_quarter_days"),

        # Previous Quarter
        quarter(add_months(col("date"), -3)).cast("string").alias("previous_quarter_calendar_quarter_number"),
        concat(lit('Q'), quarter(add_months(col("date"), -3)).cast("string")).alias("previous_quarter_calendar_quarter"),
        (datediff(add_months(col("date"), -3), date_trunc('quarter', add_months(col("date"), -3))) + 1).alias("previous_quarter_calendar_day_of_quarter"),
        date_format(date_trunc('quarter', add_months(col("date"), -3)), 'yyyy-MM-dd').alias("previous_quarter_calendar_start_of_quarter"),
        last_day(add_months(date_trunc('quarter', add_months(col("date"), -3)), 2)).alias("previous_quarter_calendar_end_of_quarter"),
        concat(year(add_months(col("date"), -3)), lpad(quarter(add_months(col("date"), -3)).cast("string"), 2, '0')).alias("previous_quarter_calendar_year_quarter_number"),
        concat(lit('Q'), quarter(add_months(col("date"), -3)).cast("string"), lit(' '), year(add_months(col("date"), -3))).alias("previous_quarter_calendar_quarter_year"),
        datediff(last_day(date_trunc('quarter', col("date"))), date_trunc('quarter', add_months(col("date"), -3))).alias("previous_quarter_calendar_quarter_days"),
        
        # Previous Year Quarter
        quarter(expr("add_months(date, -12)")).cast("string").alias("previous_year_calendar_quarter_number"),
        concat(lit('Q'), quarter(expr("add_months(date, -12)")).cast("string")).alias("previous_year_calendar_quarter"),
        (datediff(add_months(col("date"), -12), date_trunc('quarter', add_months(col("date"), -12))) + 1).alias("previous_year_calendar_day_of_quarter"),    
        date_format(date_trunc('quarter', add_months(col("date"), -12)), 'yyyy-MM-dd').alias("previous_year_calendar_start_of_quarter"),
        last_day(add_months(date_trunc('quarter', expr("add_months(date, -12)")), 2)).alias("previous_year_calendar_end_of_quarter"),
        concat(year(expr("add_months(date, -12)")), lpad(quarter(expr("add_months(date, -12)")).cast("string"), 2, '0')).alias("previous_year_calendar_year_quarter_number"),
        concat(lit('Q'), quarter(expr("add_months(date, -12)")).cast("string"), lit(' '), year(expr("add_months(date, -12)"))).alias("previous_year_calendar_quarter_year"),
        (datediff(last_day(add_months(date_trunc('quarter', add_months(col("date"), -12)), 2)), date_trunc('quarter', add_months(col("date"), -12))) + 1).alias("previous_year_calendar_quarter_days"),
        
        # Year
        year(col("date")).alias("calendar_year_number"),
        to_date(concat(year(col("date")), lit('-01-01'))).alias("calendar_start_of_year"),
        to_date(concat(year(col("date")), lit('-12-31'))).alias("calendar_end_of_year"),
        dayofyear(col("date")).alias("calendar_day_of_year_number"),
        when(month(date_add(last_day(to_date(concat(year(col("date")), lit('-12-31')))), 1)) == 2, 366).otherwise(365).alias("calendar_year_days"),
        
        # Previous Year
        year(col("date") - expr("interval 1 year")).alias("previous_year_calendar_year_number"),
        to_date(concat((year(col("date")) - 1).cast("string"), lit('-01-01'))).alias("previous_year_calendar_start_of_year"),
        to_date(concat((year(col("date")) - 1).cast("string"), lit('-12-31'))).alias("previous_year_calendar_end_of_year"),
        dayofyear(col("date") - expr("interval 1 year")).alias("previous_year_calendar_day_of_year_number"),
        when(month(date_add(last_day(to_date(concat((year(col("date")) - 1).cast("string"), lit('-12-31')))), 1)) == 2, 366).otherwise(365).alias("previous_year_calendar_year_days")
    ).distinct()

    return df_stage


# In[ ]:


# Build stage dataframe
df_stage = generate_calendar_dataframe()


# In[ ]:


# Write date table to medallion layers
df_stage.write.format('delta').mode('overwrite').saveAsTable('ent_silver.calendar_date')
df_stage.write.format('delta').mode('overwrite').saveAsTable('ent_gold.dim_calendar_date')

