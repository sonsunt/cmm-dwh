select  holiday as date, description
from {{ source('bq_coinmarketbase', 'holidays') }}