alter table cdm.dm_settlement_report
    add constraint dm_settlement_report_unique_key
        unique (settlement_date, restaurant_id);