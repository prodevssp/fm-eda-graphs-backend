import pandas as pd
from rest_framework.decorators import api_view
from rest_framework.response import Response

from .models import Document
from .forms import DocumentForm

from django.db import connection


from rest_framework.decorators import api_view
from rest_framework.response import Response


def my_custom_sql(query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
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

    print(result)

    for row in result:
        if (
            row[2] == "int"
            or row[2] == "integer"
            or row[2] == "tinyint"
            or row[2] == "smallint"
            or row[2] == "mediumint"
            or row[2] == "bigint"
            or row[2] == "unsigned big int"
            or row[2] == "int2"
            or row[2] == "int8"
            or row[2] == "real"
            or row[2] == "double"
            or row[2] == "double precision"
            or row[2] == "float"
            or row[2] == "numeric"
            or row[2] == "INT"
            or row[2] == "INTEGER"
            or row[2] == "TINYINT"
            or row[2] == "SMALLINT"
            or row[2] == "MEDIUMINT"
            or row[2] == "BIGINT"
            or row[2] == "UNSIGNED BIG INT"
            or row[2] == "INT2"
            or row[2] == "INT8"
            or row[2] == "REAL"
            or row[2] == "DOUBLE"
            or row[2] == "DOUBLE PRECISION"
            or row[2] == "FLOAT"
            or row[2] == "NUMERIC"
        ):
            features.append(row[1])

    return Response({"features": features})


@api_view(["POST"])
def calc(request):
    selected_columns = request.data["columns"]
    # features_limit = request.data["featuresLimit"]

    # if len(selected_columns) <= int(features_limit):
    columns = ",".join(word for word in selected_columns if word)

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

    newdf = df[selected_columns].copy()

    corr = newdf.corr(method=request.data["method"], numeric_only=True)

    return Response(generateHeatmapData(corr))


def generateHeatmapData(corr: pd.DataFrame):
    data = []
    headers = corr.columns.values.tolist()

    for labelY in headers:
        datum = {"id": labelY, "data": []}

        for labelX in headers:
            datum["data"].append({"x": labelX, "y": corr[labelY][labelX]})

        data.append(datum)
    return data


@api_view(["POST"])
def generatePairPlot(request):
    selected_columns = request.data["columns"]

    if len(selected_columns) < 2:
        return Response({"error": "At least two columns must be selected"}, status=400)

    columns = ",".join(selected_columns)

    query = f"SELECT {columns} FROM dummy"
    df = fetch_data(query)  # Assuming this returns a Pandas DataFrame

    # Check if the DataFrame has the expected columns
    if not all(col in df.columns for col in selected_columns):
        return Response(
            {"error": "Some selected columns are not in the DataFrame"}, status=400
        )

    # Convert the DataFrame to the desired format
    transformed_data = df[selected_columns].to_dict(orient="records")

    return Response(transformed_data)
