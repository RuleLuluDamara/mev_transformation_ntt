from flask import Flask, jsonify
from load_data_local import load_data_local
from load_data import load_data
from transformasi_data import transformasi_data, save_transformed_data
from psycopg2 import sql
from import_data import import_data_to_postgresql

app = Flask(__name__)

@app.route('/')
def home():
    return "App is running!"

@app.route('/import_mev_local', methods=['GET'])
def import_mev_local():
    try:
        df = load_data_local()
        return jsonify(df.to_dict(orient="records"))
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/import_mev', methods=['GET'])
def import_mev():
    try:
        df = load_data()
        return jsonify(df.to_dict(orient="records"))
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/transform_mev', methods=['POST'])
def transform_mev():
    try:
        df = load_data()
        transformed_df = transformasi_data(df)
        transformed_data = transformed_df.reset_index().to_dict(orient="records")
        return jsonify(transformed_data)
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/save_transformed', methods=['POST'])
def save_transformed():
    try:
        df = load_data()
        transformed_df = transformasi_data(df)
        result = save_transformed_data(transformed_df)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/save_to_postgresql', methods=['POST'])
def save_to_postgresql():
    try:
        df = load_data_local()
        result = import_data_to_postgresql(df)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)})

if __name__ == '__main__':
    app.run(debug=True)
