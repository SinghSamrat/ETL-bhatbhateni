def aggregate_sales(ctx, cs):
    query = """
    insert into samrat_bhatbhateni_olap.bhatbhateni_tgt.f_bhatbhateni_agg_sls_plc_month_t
    select 
        concat('10',s.product_id),
        concat('10',s.store_id),
        concat('10',sub.category_id),
        to_varchar(transaction_time,'MM'),
        sum(quantity),
        sum(amount),
        sum(discount),
        current_timestamp(2),
        current_timestamp(2)
    from
    bhatbhateni.transactions.sales s
    inner join
    bhatbhateni.transactions.product p
    on s.product_id = p.id
    inner join
    bhatbhateni.transactions.subcategory sub
    on p.subcategory_id = sub.id
    group by s.product_id,to_varchar(s.transaction_time,'MM'),s.store_id,sub.category_id;
    """
    cs.execute(query)
