-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "4ea4f0aa-201f-4f2b-89ac-c06461102d1e",
-- META       "default_lakehouse_name": "raqsconsole_LH",
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

-- MAGIC %%pyspark
-- MAGIC 
-- MAGIC # Automatic schema evolution on queries
-- MAGIC 
-- MAGIC spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

use schema dbo

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- dimension table Accounts

create or replace table AdventureWorks_LS.dbo.dim_accounts
using delta as

select distinct
    AccountKey,
    AccountDescription,
    AccountType,
    ValueType,
    Operator,
    current_timestamp() as load_time
from 
    AdventureWorks_LS.bronze.adventureworks_dimaccount
where 
    AccountType is not null;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- dimension table Customers

create or replace table AdventureWorks_LS.dbo.dim_customer
using delta as

select
     CustomerKey as customerId
    ,CustomerAlternateKey as customerNumber
	,concat_ws(' ', FirstName, MiddleName, LastName) as CustomerName
    ,BirthDate as DateofBirth
	,case when MaritalStatus = 'M' then 'Male' else 'Female' end as MaritalStatus
    ,case when Gender = 'M' then 'Male' else 'Female' end as Gender
    ,EmailAddress
    ,YearlyIncome
    ,TotalChildren
    ,AddressLine1 customerAddress
    ,Phone
    ,CommuteDistance
from 
    AdventureWorks_LS.bronze.adventureworks_dimcustomer

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--fact table ft_internetsales

create table if not exists AdventureWorks_LS.dbo.ft_internetsales
using delta as

select
   OrderDateKey
   ,DueDateKey
   ,ShipDateKey
   ,CustomerKey
   ,SalesOrderNumber
   ,SalesOrderLineNumber
   ,OrderQuantity
   ,UnitPrice
   ,DiscountAmount
   ,ProductStandardCost
   ,TotalProductCost
   ,SalesAmount
from 
    AdventureWorks_LS.bronze.adventureworks_factinternetsales
where 1 = 0;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

merge into AdventureWorks_LS.dbo.ft_internetsales as target 
using (
    select *
    from
        AdventureWorks_LS.bronze.adventureworks_factinternetsales
    where OrderDateKey > coalesce( (select max(OrderDateKey) from AdventureWorks_LS.dbo.ft_internetsales), '1900-01-01')
) as source 
on target.SalesOrderNumber = source.SalesOrderNumber


WHEN MATCHED THEN
    UPDATE SET
        target.OrderDateKey = source.OrderDateKey,
        target.DueDateKey = source.DueDateKey,
        target.ShipDateKey = source.ShipDateKey,
        target.CustomerKey = source.CustomerKey,
        target.OrderQuantity = source.OrderQuantity,
        target.UnitPrice = source.UnitPrice,
        target.DiscountAmount = source.DiscountAmount,
        target.ProductStandardCost = source.ProductStandardCost,
        target.TotalProductCost = source.TotalProductCost,
        target.SalesAmount = source.SalesAmount

WHEN NOT MATCHED THEN
    INSERT (
        OrderDateKey, DueDateKey, ShipDateKey, CustomerKey, SalesOrderNumber,
        SalesOrderLineNumber, OrderQuantity, UnitPrice, DiscountAmount,
        ProductStandardCost, TotalProductCost, SalesAmount
    )
    VALUES (
        source.OrderDateKey, source.DueDateKey, source.ShipDateKey, source.CustomerKey, source.SalesOrderNumber,
        source.SalesOrderLineNumber, source.OrderQuantity, source.UnitPrice, source.DiscountAmount,
        source.ProductStandardCost, source.TotalProductCost, source.SalesAmount
    );

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
