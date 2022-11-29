from extraction import delete_records, extraction, show
import pandas as pd
from cursor import create_cursor
from exporter import export_sales, export_location
from transformation import transformation
from loading import loading
from aggregate_sales import aggregate_sales

ctx, cs = create_cursor()


extraction(ctx, cs)
# export_sales(ctx, cs)
# export_location(ctx, cs)
transformation(ctx, cs)
loading(ctx, cs)
aggregate_sales(ctx, cs)
