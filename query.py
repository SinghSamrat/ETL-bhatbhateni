def extraction_sales():
    query = """insert into 
               samrat_bhatbhateni_olap.transactions.d_bhatbhateni_sls_stg
               select * from bhatbhateni.transactions.sales"""
    return query


def extraction_country():
    query = "insert into samrat_bhatbhateni_olap.transactions.d_bhatbhateni_cntry_stg select * from bhatbhateni.transactions.country"
    return query


def extraction_region():
    query = "insert into samrat_bhatbhateni_olap.transactions.d_bhatbhateni_rgn_stg select * from bhatbhateni.transactions.region"
    return query


def extraction_store():
    query = "insert into samrat_bhatbhateni_olap.transactions.d_bhatbhateni_str_stg select * from bhatbhateni.transactions.store"
    return query


def extraction_category():
    query = "insert into samrat_bhatbhateni_olap.transactions.d_bhatbhateni_ctgry_stg select * from bhatbhateni.transactions.category"
    return query


def extraction_subcategory():
    query = "insert into samrat_bhatbhateni_olap.transactions.d_bhatbhateni_sub_ctgry_stg select * from bhatbhateni.transactions.subcategory"
    return query


def extraction_product():
    query = "insert into samrat_bhatbhateni_olap.transactions.d_bhatbhateni_pdt_stg select * from bhatbhateni.transactions.product"
    return query


def extraction_customer():
    query = "insert into samrat_bhatbhateni_olap.transactions.d_bhatbhateni_customer_stg select * from bhatbhateni.transactions.customer"
    return query


def transformation_country():
    query = """
    merge into samrat_bhatbhateni_olap.bhatbhateni_tmp.d_bhatbhateni_cntry_tmp t
    using samrat_bhatbhateni_olap.transactions.d_bhatbhateni_cntry_stg s
    on t.cntry_id = s.id
    when matched then
    update set
    t.row_updt_tms = current_timestamp(2)
    when not matched then
    insert  values
    (
        s.id,
        concat('10',s.id),
        s.country_desc,
        'O'
        current_timestamp(2),
        current_timestamp(2)
    );
    """
    return query


def transformation_region():
    query = """
    merge into samrat_bhatbhateni_olap.bhatbhateni_tmp.d_bhatbhateni_rgn_tmp t
    using samrat_bhatbhateni_olap.transactions.d_bhatbhateni_rgn_stg s
    on t.rgn_id = s.id 
    when matched then
    update set
    t.row_updt_tms = current_timestamp(2)
    when not matched then
    insert 
    values
    (
        s.id,
        concat('10',s.id),
        concat('10',s.country_id),
        s.region_desc,
        'O',
        current_timestamp(2),
        current_timestamp(2)
    );
    """
    return query


def transformation_location():
    query = """
    merge into samrat_bhatbhateni_olap.bhatbhateni_tmp.d_bhatbhateni_locn_tmp t
    using samrat_bhatbhateni_olap.transactions.d_bhatbhateni_str_stg s
    on t.locn_id = s.id 
    when matched then
    update set
    t.row_updt_tms = current_timestamp(2)
    when not matched then
    insert 
    values
    (
        s.id,
        concat('10',s.id),
        concat('10',s.region_id),
        s.store_desc,
        dateadd(day,-1,current_date),
        current_date,
        'Y',
        'O',
        current_timestamp(2),
        current_timestamp(2)
    );
    """
    return query


def transformation_category():
    query = """
    merge into samrat_bhatbhateni_olap.bhatbhateni_tmp.d_bhatbhateni_ctgry_tmp t
    using samrat_bhatbhateni_olap.transactions.d_bhatbhateni_ctgry_stg s
    on t.ctgry_id = s.id
    when matched then
    update set
    t.row_updt_tms = current_timestamp(2)
    when not matched then
    insert values
    (
        s.id,
        concat('10',s.id),
        s.category_desc,
        'O',
        current_timestamp(2),
        current_timestamp(2)
    )
    """
    return query


def transformation_subcategory():
    query = """
    merge into samrat_bhatbhateni_olap.bhatbhateni_tmp.d_bhatbhateni_sub_ctgry_tmp t
    using samrat_bhatbhateni_olap.transactions.d_bhatbhateni_sub_ctgry_stg s
    on t.sub_ctgry_id = s.id
    when matched then
    update set
    t.row_updt_tms = current_timestamp(2)
    when not matched then
    insert values
    (
        s.id,
        concat('10',s.id),
        concat('10',s.category_id),
        s.subcategory_desc,
        'O',
        current_timestamp(2),
        current_timestamp(2)
    )
    """
    return query


def transformation_product():
    query = """
    merge into samrat_bhatbhateni_olap.bhatbhateni_tmp.d_bhatbhateni_pdt_tmp t
    using samrat_bhatbhateni_olap.transactions.d_bhatbhateni_pdt_stg s
    on t.pdt_id = s.id
    when matched then
    update set
    t.row_updt_tms = current_timestamp(2)
    when not matched then
    insert values
    (
        s.id,
        concat('10',s.id),
        concat('10',s.subcategory_id),
        s.product_desc,
        (select sum(sls.amount)/sum(sls.quantity) from samrat_bhatbhateni_olap.transactions.d_bhatbhateni_sls_stg sls where sls.product_id = s.id),
        'Y',
        'O',
        current_timestamp(2),
        current_timestamp(2)
    );
    """
    return query


def transformation_customer():
    query = """
    merge into samrat_bhatbhateni_olap.bhatbhateni_tmp.d_bhatbhateni_customer_tmp t
    using samrat_bhatbhateni_olap.transactions.d_bhatbhateni_customer_stg s
    on t.customer_id = s.id
    when matched then
    update set
    t.row_updt_tms = current_timestamp(2)
    when not matched then
    insert values
    (
        s.id,
        '10'||s.id,
        s.customer_first_name,
        s.customer_middle_name,
        s.customer_last_name,
        s.customer_address,
        'O',
        current_timestamp(2),
        current_timestamp(2)
    );
    """
    return query


def transformation_sales():
    query = """
    merge into samrat_bhatbhateni_olap.bhatbhateni_tmp.f_bhatbhateni_sls_tmp t
    using samrat_bhatbhateni_olap.transactions.d_bhatbhateni_sls_stg s
    on t.sls_id = s.id
    when matched then
    update set
    t.row_updt_tms = current_timestamp(2)
    when not matched then
    insert values
    (
        s.id,
        s.store_id,
        s.product_id,
        '10'||s.customer_id,
        s.transaction_time,
        s.quantity,
        s.amount,
        s.discount,
        'O',
        current_timestamp(2),
        current_timestamp(2)
    );
    """
    return query


def load_country():
    query = """
    insert into
    samrat_bhatbhateni_olap.bhatbhateni_tgt.d_bhatbhateni_cntry_t
    select * from 
    samrat_bhatbhateni_olap.bhatbhateni_tmp.d_bhatbhateni_cntry_tmp
    """
    return query


def load_region():
    query = """
    insert into
    samrat_bhatbhateni_olap.bhatbhateni_tgt.d_bhatbhateni_rgn_t
    select * from 
    samrat_bhatbhateni_olap.bhatbhateni_tmp.d_bhatbhateni_rgn_tmp
    """
    return query


def load_location():
    query = """
    insert into
    samrat_bhatbhateni_olap.bhatbhateni_tgt.d_bhatbhateni_locn_t
    select * from 
    samrat_bhatbhateni_olap.bhatbhateni_tmp.d_bhatbhateni_locn_tmp
    """
    return query


def load_category():
    query = """
    insert into
    samrat_bhatbhateni_olap.bhatbhateni_tgt.d_bhatbhateni_ctgry_t
    select * from 
    samrat_bhatbhateni_olap.bhatbhateni_tmp.d_bhatbhateni_ctgry_tmp
    """
    return query


def load_subcategory():
    query = """
    insert into
    samrat_bhatbhateni_olap.bhatbhateni_tgt.d_bhatbhateni_sub_ctgry_t
    select * from 
    samrat_bhatbhateni_olap.bhatbhateni_tmp.d_bhatbhateni_sub_ctgry_tmp
    """
    return query


def load_product():
    query = """
    insert into
    samrat_bhatbhateni_olap.bhatbhateni_tgt.d_bhatbhateni_pdt_t
    select * from 
    samrat_bhatbhateni_olap.bhatbhateni_tmp.d_bhatbhateni_pdt_tmp
    """
    return query


def load_customer():
    query = """
    insert into
    samrat_bhatbhateni_olap.bhatbhateni_tgt.d_bhatbhateni_customer_t
    select * from 
    samrat_bhatbhateni_olap.bhatbhateni_tmp.d_bhatbhateni_customer_tmp
    """
    return query


def load_sales():
    query = """
    insert into
    samrat_bhatbhateni_olap.bhatbhateni_tgt.f_bhatbhateni_sls_t
    select * from 
    samrat_bhatbhateni_olap.bhatbhateni_tmp.f_bhatbhateni_sls_tmp
    """
    return query


def delete_all_records(table_name):
    query = "delete from " + str(table_name)
    return query


def show_sales_data():
    query = "select * from  samrat_bhatbhateni_olap.transactions.d_bhatbhateni_str_stg"
    return query


value = extraction_sales()
