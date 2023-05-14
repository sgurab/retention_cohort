with date_range as (
  select
    date_trunc(calendar_day, month) as calendar_month
            
  from 
    unnest(generate_date_array('2022-01-01', current_date, interval 1 day)) as calendar_day
            
  group by
    calendar_month
)
-- Base de estado atual dos clientes

, base_one as (    
  select DISTINCT
    client_id,
    created_at as start_at,
    churn_at,
    date(date_trunc(created_at, month)) as cohort,

  from `timeline_clients` as tc

  where
    is_current_state
)
-- Base mensal dos clientes

, monthly_cis as (
  select
    calendar_month,
    cohort,
    date_diff(calendar_month,cohort,month) as months_since_creation,
    base_sch.client_id,
    count(distinct if(churn_at is not null and date_trunc(churn_at,month) <= calendar_month, client_id,null)) as churn_count, 
    count(distinct client_id) as escolas
  
  from base_sch
  cross join date_range

  where 
    calendar_month >= "2022-01-01" and cohort >= "2022-01-01"
    and date_diff(calendar_month,cohort,month) >= 0
  
  group by 
    calendar_month, 
    cohort, 
    client_id
  
  order by 
    client_id, 
    calendar_month
)
-- Base final
select
  months_since_creation,
  cohort,
  sum(escolas) as escolas_safra,
  sum(churn_count) as churns,
  (sum(escolas) - sum(churn_count))/sum(escolas) as retention_rate

from monthly_cis

group by 
  months_since_creation, 
  cohort

order by 
  cohort, 
  months_since_creation
