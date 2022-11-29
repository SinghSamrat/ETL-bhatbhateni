from cursor import create_cursor
import pandas as pd
import query

ctx, cs = create_cursor()


def format(input):
    result = pd.DataFrame(input)
    return result


def delete_records(table_name):
    delete = query.delete_all_records(table_name)
    cs.execute(delete)


def extraction(ctx, cs):

    try:
        extraction_sls = query.extraction_sales()
        print("extracting sales data")
        cs.execute(extraction_sls)

        extraction_cntry = query.extraction_country()
        print("extracting country data")
        cs.execute(extraction_cntry)

        extraction_rgn = query.extraction_region()
        print("extracting region data")
        cs.execute(extraction_rgn)

        extraction_str = query.extraction_store()
        print("extracting store data")
        cs.execute(extraction_str)

        extraction_ctgry = query.extraction_category()
        print("extracting category data")
        cs.execute(extraction_ctgry)

        extraction_sub_ctgry = query.extraction_subcategory()
        print("extracting subcategory data")
        cs.execute(extraction_sub_ctgry)

        extraction_pdt = query.extraction_product()
        print("extracting product data")
        cs.execute(extraction_pdt)

        extraction_customer = query.extraction_customer()
        print("extracting customer data")
        cs.execute(extraction_customer)

        print("EXTRACTION SUCCESSFUL")
        print("\n")

    except Exception as e:
        print("EXTRACTION FAILED:" + " " + str(e))


def show(ctx, cs):
    show_sls = query.show_sales_data()
    # print("showing sales data")
    result = cs.execute(show_sls)

    print(format(result))
