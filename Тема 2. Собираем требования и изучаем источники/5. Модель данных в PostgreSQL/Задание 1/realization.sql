SELECT distinct elem->>'product_name' AS product_name
FROM public.outbox o,
     jsonb_array_elements((o.event_value::jsonb)->'product_payments') AS elem;