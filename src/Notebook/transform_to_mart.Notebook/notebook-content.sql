-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "4ea4f0aa-201f-4f2b-89ac-c06461102d1e",
-- META       "default_lakehouse_name": "dev_raqsconsole_LH",
-- META       "default_lakehouse_workspace_id": "6df05e67-30af-48c3-8841-b1aa8f82e95e",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "4ea4f0aa-201f-4f2b-89ac-c06461102d1e"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- DROP DIMENSION TABLE IF EXISTS
drop table if exists dev_raqsconsole_LH.dbo.dim_customer;

-- CREATE DIMENSION TABLE
create table dev_raqsconsole_LH.dbo.dim_customer (
    customerkey int,
    customernumber string,
    customername string,
    dateofbirth date,
    maritalstatus string,
    gender string,
    emailaddress string,
    yearlyincome decimal(18,2),
    totalchildren int,
    phone string,
    customeraddress string,
    city string,
    province string,
    country string,
    commutedistance string,
    snapshot_date date,
    _ingested_at timestamp
)
partitioned by (snapshot_date);

-- INSERT LATEST SNAPSHOT
insert overwrite table dev_raqsconsole_LH.dbo.dim_customer
partition(snapshot_date)

select
    cast(cus.customerkey as int) as customerkey,
    cast(cus.customeralternatekey as string) as customernumber,
    concat_ws(' ', cus.firstname, cus.middlename, cus.lastname) as customername,
    cast(cus.birthdate as date) as dateofbirth,
    case when lower(cus.maritalstatus) = 'm' then 'married' else 'single' end as maritalstatus,
    case when lower(cus.gender) = 'm' then 'male' else 'female' end as gender,
    cus.emailaddress as emailaddress,
    cast(cus.yearlyincome as decimal(18,2)) as yearlyincome,
    cast(cus.totalchildren as int) as totalchildren,
    cus.phone as phone,
    cus.addressline1 as customeraddress,
    geo.city as city,
    geo.stateprovincename as province,
    geo.englishcountryregionname as country,
    cus.commutedistance as commutedistance,
    cast(cus.snapshot_date as date) as snapshot_date,
    current_timestamp() as _ingested_at
from dev_raqsconsole_LH.bronze.adventureworks_dimcustomer as cus
left join dev_raqsconsole_LH.bronze.adventureworks_dimgeography as geo
    on cus.geographykey = geo.geographykey
where cus.snapshot_date = (
    select max(snapshot_date) 
    from dev_raqsconsole_LH.bronze.adventureworks_dimcustomer
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

drop table if exists dev_raqsconsole_LH.dbo.dim_product;

create table dev_raqsconsole_LH.dbo.dim_product (
    product_key int,
    product_name string,
    status string,
    sub_category string,
    product_category string,
    snapshot_date date,
    _ingested_at timestamp
)
partitioned by (snapshot_date);

-- INSERT LATEST SNAPSHOT
insert overwrite table dev_raqsconsole_LH.dbo.dim_product
partition(snapshot_date)

with 
max_date as (
    select max(snapshot_date) as snapshot_date
    from dev_raqsconsole_LH.bronze.adventureworks_dimproduct
),

cte_prodjoin as(

    select 
        prd.ProductKey as product_key,
        prd.EnglishProductName as product_name, 
        prd.status,
        sub.ProductSubcategoryName as sub_category,
        cat.ProductCategoryName as product_category,
        row_number() over(partition by prd.ProductKey order by prd.ProductKey) as remove_duplicate,
        cast(prd.snapshot_date as date) as snapshot_date,
        current_timestamp() as _ingested_at
    from
        dev_raqsconsole_LH.bronze.adventureworks_dimproduct as prd
    left join
        dev_raqsconsole_LH.bronze.adventureworks_dimproductsubcategory as sub
        on prd.ProductSubcategoryKey = sub.ProductSubcategoryKey
    left join
        dev_raqsconsole_LH.bronze.adventureworks_dimproductcategory as cat
        on sub.ProductCategoryKey = cat.ProductCategoryKey
    cross join max_date
    where prd.snapshot_date = max_date.snapshot_date

)

select product_key, product_name, status, sub_category, product_category, snapshot_date, _ingested_at
from cte_prodjoin
where remove_duplicate = 1
and product_key is not null;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- CREATE FACT TABLE IF NOT EXISTS
create table if not exists dev_raqsconsole_LH.dbo.ft_internetsales (
    SalesOrderNumber string,
    SalesOrderLineNumber int,
    OrderDateKey date,
    DueDateKey date,
    ShipDateKey date,
    CustomerKey int,
    ProductKey int,
    OrderQuantity int,
    UnitPrice decimal(18,2),
    DiscountAmount decimal(18,2),
    ProductStandardCost decimal(18,2),
    TotalProductCost decimal(18,2),
    SalesAmount decimal(18,2),
    snapshot_date date,
    _ingested_at timestamp
)
partitioned by (snapshot_date);


merge into dev_raqsconsole_LH.dbo.ft_internetsales as target
using (
    select *
    from dev_raqsconsole_LH.bronze.adventureworks_factinternetsales
    where OrderDateKey > coalesce(
        (select max(OrderDateKey) from dev_raqsconsole_LH.dbo.ft_internetsales),
        date '1900-01-01'
    )
) as source
on target.SalesOrderNumber = source.SalesOrderNumber
   and target.SalesOrderLineNumber = source.SalesOrderLineNumber

when matched then
    update set
        target.OrderDateKey = source.OrderDateKey,
        target.DueDateKey = source.DueDateKey,
        target.ShipDateKey = source.ShipDateKey,
        target.CustomerKey = source.CustomerKey,
        target.ProductKey = source.ProductKey,
        target.OrderQuantity = source.OrderQuantity,
        target.UnitPrice = source.UnitPrice,
        target.DiscountAmount = source.DiscountAmount,
        target.ProductStandardCost = source.ProductStandardCost,
        target.TotalProductCost = source.TotalProductCost,
        target.SalesAmount = source.SalesAmount,
        target._ingested_at = current_timestamp(),
        target.snapshot_date = source.snapshot_date

when not matched then
    insert (
        SalesOrderNumber, SalesOrderLineNumber, OrderDateKey, DueDateKey, ShipDateKey,
        CustomerKey, ProductKey, OrderQuantity, UnitPrice, DiscountAmount,
        ProductStandardCost, TotalProductCost, SalesAmount, snapshot_date, _ingested_at
    )
    values (
        source.SalesOrderNumber, source.SalesOrderLineNumber, source.OrderDateKey,
        source.DueDateKey, source.ShipDateKey, source.CustomerKey, source.ProductKey,
        source.OrderQuantity, source.UnitPrice, source.DiscountAmount, source.ProductStandardCost,
        source.TotalProductCost, source.SalesAmount, source.snapshot_date, current_timestamp()
    );

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- Create fact table if not exists
create table if not exists dev_raqsconsole_LH.dbo.ft_product_inventory (
    inventory_key string,
    product_key int,
    date_key date,
    movement_date date,
    unit_cost decimal(18,2),
    unit_in int,
    unit_out int,
    unit_balance int,
    snapshot_date date,
    _ingested_at timestamp
)
partitioned by (snapshot_date);


-- Merge logic using CTE
with source_data as (
    select 
        concat_ws('', ProductKey, DateKey) as inventory_key,
        ProductKey as product_key,
        DateKey as date_key,
        MovementDate as movement_date,
        UnitCost as unit_cost,
        UnitsIn as unit_in,
        UnitsOut as unit_out,
        UnitsBalance as unit_balance,
        current_date() as snapshot_date
    from dev_raqsconsole_LH.bronze.adventureworks_factproductinventory
),
filtered_source as (
    select *
    from source_data
    where date_key > coalesce(
        (select max(date_key) from dev_raqsconsole_LH.dbo.ft_product_inventory),
        date '1900-01-01'
    )
)

merge into dev_raqsconsole_LH.dbo.ft_product_inventory as target
using filtered_source as source
on target.inventory_key = source.inventory_key

when matched then
    update set
        target.product_key = source.product_key,
        target.date_key = source.date_key,
        target.movement_date = source.movement_date,
        target.unit_cost = source.unit_cost,
        target.unit_in = source.unit_in,
        target.unit_out = source.unit_out,
        target.unit_balance = source.unit_balance,
        target.snapshot_date = source.snapshot_date,
        target._ingested_at = current_timestamp()

when not matched then
    insert (
        inventory_key, product_key, date_key, movement_date, unit_cost,
        unit_in, unit_out, unit_balance, snapshot_date, _ingested_at
    )
    values (
        source.inventory_key, source.product_key, source.date_key, source.movement_date,
        source.unit_cost, source.unit_in, source.unit_out, source.unit_balance,
        source.snapshot_date, current_timestamp()
    );


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
