import pandas as pd
from rest_framework.decorators import api_view
from rest_framework.response import Response

from .models import Document
from .forms import DocumentForm

from django.db import connection


def my_custom_sql(query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
    # print(rows)
    return rows


def fetch_data(sql_query):
    with connection.cursor() as cursor:
        cursor.execute(sql_query)
        columns = [col[0] for col in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=columns)


@api_view(["GET", "POST"])
def hello_world(request):
    if request.method == "POST":
        return Response({"message": "Got some data!", "data": request.data})

    return Response({"message": "Hello, world!"})


@api_view(["POST"])
def upload(request):
    form = DocumentForm(request.POST, request.FILES)
    if form.is_valid():
        newdoc = Document(docfile=request.FILES["file"])
        newdoc.save()

        try:
            df = pd.read_csv(newdoc.docfile.path)
        except UnicodeDecodeError:
            df = pd.read_csv(newdoc.docfile.path, encoding="latin")

        response = {
            "file": newdoc.docfile.path,
            "columns": df.columns.values.tolist(),
            "status": "success",
        }

        return Response(response)


@api_view(["GET"])
def get_features(request):
    result = my_custom_sql("PRAGMA table_info(dummy)")

    features = []

    for row in result:
        if row[2] == "INTEGER":
            features.append(row[1])

    return Response({"features": features})


@api_view(["POST"])
def calc(request):
    columns = ",".join(word for word in request.data["columns"] if word)

    query = f"SELECT {columns} FROM dummy"

    print(request.data)

    start_date = request.data.get("startDate")
    end_date = request.data.get("endDate")

    print(f"Start date: {start_date}")

    if start_date:
        query += f" WHERE date >= '{start_date}' "
        if end_date:
            query += f" AND date <= '{end_date}' "

    print("Query: " + query)

    df = fetch_data(query)

    selected_columns = request.data["columns"]
    newdf = df[selected_columns].copy()

    corr = newdf.corr(method=request.data["method"], numeric_only=True)

    return Response(generate_heatmap(corr))


def generate_heatmap(corr: pd.DataFrame):
    data = []
    headers = corr.columns.values.tolist()

    for labelY in headers:
        datum = {"id": labelY, "data": []}

        for labelX in headers:
            datum["data"].append({"x": labelX, "y": corr[labelY][labelX]})

        data.append(datum)
    return data
