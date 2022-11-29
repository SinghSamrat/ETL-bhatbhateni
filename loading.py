import query


def loading(ctx, cs):
    try:
        load_country = query.load_country()
        print("Loading Country Table")
        cs.execute(load_country)

        load_region = query.load_region()
        print("Loading Region Table")
        cs.execute(load_region)

        load_location = query.load_location()
        print("Loading Country Table")
        cs.execute(load_location)

        load_category = query.load_category()
        print("Loading Category Table")
        cs.execute(load_category)

        load_subcategory = query.load_subcategory()
        print("Loading Subcategory Table")
        cs.execute(load_subcategory)

        load_product = query.load_product()
        print("Loading Product Table")
        cs.execute(load_product)

        load_customer = query.load_customer()
        print("Loading Customer Table")
        cs.execute(load_customer)

        load_sales = query.load_sales()
        print("Loading Sales Table")
        cs.execute(load_sales)

        print("LOADING SUCCESSFUL \n")

    except Exception as e:
        print("LOADING FAILED: " + str(e))
