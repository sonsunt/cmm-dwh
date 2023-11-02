select  row_number() over () as id,
        sn.id as coin_id,
        name, 
        symbol
from {{ ref('coins_snapshot') }} as sn