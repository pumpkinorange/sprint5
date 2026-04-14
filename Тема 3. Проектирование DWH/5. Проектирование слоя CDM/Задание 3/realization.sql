alter table cdm.dm_settlement_report
    add constraint dm_settlement_report_settlement_date_check
        check (settlement_date >= '2022-01-01' and settlement_date < '2500-01-01')