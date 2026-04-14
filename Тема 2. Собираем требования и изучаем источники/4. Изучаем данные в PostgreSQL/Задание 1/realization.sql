select 
	c.table_name,
	c.column_name,
	c.data_type,
	c.character_maximum_length,
	c.column_default,
	c.is_nullable
from information_schema.columns c
join information_schema.tables t 
			on c.table_name = t.table_name
			and c.table_schema = t.table_schema
where t.table_schema = 'public'
order by c.table_name