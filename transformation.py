import query
import pandas


def transformation(ctx, cs):
    try:
        transformation_cntry = query.transformation_country()
        print("Transforming Country Table")
        result = cs.execute(transformation_cntry)

        transformation_rgn = query.transformation_region()
        print("Transforming Region Table")
        result = cs.execute(transformation_rgn)

        transformation_locn = query.transformation_location()
        print("Transforming Location Table")
        result = cs.execute(transformation_locn)

        transformation_ctgry = query.transformation_category()
        print("Transforming Category Table")
        result = cs.execute(transformation_ctgry)

        transformation_sub_ctgry = query.transformation_subcategory()
        print("Transforming SubCategory Table")
        result = cs.execute(transformation_sub_ctgry)

        transformation_pdt = query.transformation_product()
        print("Transforming Product Table")
        result = cs.execute(transformation_pdt)

        transformation_customer = query.transformation_customer()
        print("Transforming Customer Table")
        result = cs.execute(transformation_customer)

        transformation_sales = query.transformation_sales()
        print("Transforming Sales Table")
        result = cs.execute(transformation_sales)

        print("TRANSFORMATION SUCCESSFUL \n")

        # df = cs.fetch_pandas_all()
        # df.to_csv(r"ggg.csv", sep="|")
    except Exception as e:
        print("TRANSFORMATION FAILURE" + str(e))
