import pandas as pd

# query = pd.read_sql_query("select * from bhatbhateni.transactions.customer")
# df = pd.DataFrame(query)
# df.to_csv(r"sales.csv", index=False)
# print(df)


def export_sales(ctx, cs):
    cs.execute("select * from bhatbhateni.transactions.sales")
    pd_df = cs.fetch_pandas_all()
    pd_df.to_csv(r"sales.csv", sep="|", index=False)

    df = pd.read_csv("sales.csv")
    print(df)


def export_location(ctx, cs):
    cs.execute("select * from bhatbhateni.transactions.store")
    pd_df = cs.fetch_pandas_all()
    pd_df.to_csv(r"location_store.csv", sep="|", index=False)

    cs.execute("select * from bhatbhateni.transactions.region")
    pd_df = cs.fetch_pandas_all()
    pd_df.to_csv(r"location_region.csv", sep="|", index=False)

    cs.execute("select * from bhatbhateni.transactions.country")
    pd_df = cs.fetch_pandas_all()
    pd_df.to_csv(r"location_country.csv", sep="|", index=False)

    df = pd.read_csv("location_country.csv")
    print(df)
