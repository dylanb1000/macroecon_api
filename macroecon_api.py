from sqlalchemy import create_engine, text
from flask import Flask, request, jsonify
import datetime

# MySQL connection info
user = 'root'
password = 'password'
host = 'db'
port = 3306
database = 'MACROECON'
econ_id_list = [
    "gdp",
    "payems",
    "unrate",
    "cpiaucsl",
    "pcepi",
    "indpro",
    "rsafs",
    "houst",
    "exhoslusm495s",
    "cpatax"]
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')

def get_econ_data(econ_string,date_string):
    with engine.connect() as conn:
        column_description = conn.execute(text("SELECT description FROM macroecon_description where symbol = '{}'".format(econ_string))).fetchone()
        result = conn.execute(
        text('SELECT {} FROM macroecon_values WHERE `index` = :date'.format(econ_string)),
        {"date": date_string}
        ).fetchone()
    return [column_description, result]

def get_json_econ(econ_string,date_string):
    try:
        result = get_econ_data(econ_string,datetime.datetime.strptime(date_string, "%Y-%m-%d"))
        return jsonify({"symbol":econ_string,"description":"{}".format(result[0][0]),"date":date_string,"data":"{}".format(result[1][0]),"other_symbols":econ_id_list}), 200
    except:
        return jsonify({"error": "Item not found"}), 404

app = Flask(__name__)

@app.route('/api/econ/gdp/<string:date_string>', methods=['GET'])
def get_gdp(date_string):
    return get_json_econ("gdp",date_string)
    
@app.route('/api/econ/payems/<string:date_string>', methods=['GET'])
def get_payems(date_string):
    return get_json_econ("payems",date_string)
    
@app.route('/api/econ/unrate/<string:date_string>', methods=['GET'])
def get_unrate(date_string):
    return get_json_econ("unrate",date_string)
    
@app.route('/api/econ/cpiaucsl/<string:date_string>', methods=['GET'])
def get_cpiaucsl(date_string):
    return get_json_econ("cpiaucsl",date_string)
    
@app.route('/api/econ/pcepi/<string:date_string>', methods=['GET'])
def get_pcepi(date_string):
    return get_json_econ("pcepi",date_string)
    
@app.route('/api/econ/indpro/<string:date_string>', methods=['GET'])
def get_indpro(date_string):
    return get_json_econ("indpro",date_string)

@app.route('/api/econ/rsafs/<string:date_string>', methods=['GET'])
def get_rsafs(date_string):
    return get_json_econ("rsafs",date_string)
    
@app.route('/api/econ/houst/<string:date_string>', methods=['GET'])
def get_houst(date_string):
    return get_json_econ("houst",date_string)
    
@app.route('/api/econ/exhoslusm495s/<string:date_string>', methods=['GET'])
def get_exhoslusm(date_string):
    return get_json_econ("exhoslusm495s",date_string)
    
@app.route('/api/econ/cpatax/<string:date_string>', methods=['GET'])
def get_cpatax(date_string):
    return get_json_econ("cpatax",date_string)
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
