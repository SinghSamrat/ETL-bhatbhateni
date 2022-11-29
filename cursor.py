import snowflake.connector


def create_cursor():
    ctx = snowflake.connector.connect(
        user="SamratSingh",
        password="28852@sS",
        account="qo81549.ap-south-1.aws",
        warehouse="samrat_wrk3",
        # database="bhatbhateni",
        # schema="transactions",
    )
    cs = ctx.cursor()
    return ctx, cs


def execute_query(SQL):
    cs = create_cursor()
    result = cs.execute(SQL)
    return result
