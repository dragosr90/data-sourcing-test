select  on_prem.LAST_PERFORMING_MODEL, new.LastPerformingModel, * from bsrc_d.stg_202412.dial_fair_pd_ucr_actual as on_prem
inner join bsrc_d.int_202412.pd_ucr_actual as new
on on_prem.RATING_ID = new.RATING_ID
where on_prem.LAST_PERFORMING_MODEL <> new.LastPerformingModel
