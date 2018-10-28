from flask import Flask, jsonify, request
from flask.views import MethodView
from flask_sqlalchemy import SQLAlchemy
import boto3
from botocore.client import Config
import requests

from config import aws_access_key_id, aws_secret_access_key, aws_region_name, bucket_name

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///assets.sqlite3'
db = SQLAlchemy(app)


class Assets(db.Model):
    asset_id = db.Column('asset_id', db.Integer, primary_key=True)
    uploaded = db.Column('uploaded', db.Boolean)
    url = db.Column('url', db.String())

    def __init__(self):
        self.url = ""
        self.uploaded = False


def generate_next_id():
    asset = Assets()
    db.session.add(asset)
    db.session.commit()
    return asset.asset_id


def get_asset_url(asset_id):
    asset = Assets.query.get(asset_id)
    if asset is not None:
        return asset.url
    else:
        return -1


def set_asset_url(asset_id, url):
    asset = Assets.query.get(asset_id)
    asset.url = url
    db.session.commit()


def set_asset_status(asset_id, uploaded):
    asset = Assets.query.get(asset_id)
    asset.uploaded = uploaded
    db.session.commit()


def get_asset_status(asset_id):
    asset = Assets.query.get(asset_id)
    if asset is not None:
        return asset.uploaded
    else:
        return -1


def get_s3_client():
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region_name,
        config=Config(signature_version='s3v4'))
    return s3


class AssetAPI(MethodView):
    def get(self, asset_id):
        try:
            status = get_asset_status(asset_id)
            if status == -1:
                return jsonify(message="No asset exists!"), 404
            if not status:
                return jsonify(message="No file found!"), 404

            timeout = 60
            if 'timeout' in request.args:
                timeout = request.args['timeout']

            s3 = get_s3_client()
            url = s3.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': bucket_name,
                    'Key': str(asset_id)
                },
                HttpMethod='GET',
                ExpiresIn=timeout
            )

            return jsonify(download_url=url)
        except:
            return jsonify(message="Error occured! Please try after some time!"), 500

    def post(self):
        try:
            asset_id = generate_next_id()

            s3 = get_s3_client()
            url = s3.generate_presigned_url(
                ClientMethod='put_object',
                Params={
                    'Bucket': bucket_name,
                    'Key': str(asset_id)
                },
                HttpMethod='PUT'
            )

            set_asset_url(asset_id, url)
            return jsonify(upload_url=url, id=asset_id)
        except:
            return jsonify(message="Error occured! Please try after some time!"), 500

    def put(self, asset_id):
        try:
            url = get_asset_url(asset_id)
            if url == -1:
                return jsonify(message="No asset exists!"), 404
            if 'file' in request.files:
                file = request.files['file']
                files = {"file": file}
                response = requests.put(url, files=files)
                if response.status_code != 200:
                    return jsonify(message="Error uploading a file!"), response.status_code

                set_asset_status(asset_id, True)
                return jsonify(status="uploaded")
            else:
                return jsonify(message="No 'file' object found in the request! Aborting!"), 400
        except:
            return jsonify(message="Error occured! Please try after some time!"), 500


if __name__ == "__main__":
    db.create_all()
    asset_view = AssetAPI.as_view('asset_api')
    app.add_url_rule('/asset/', view_func=asset_view, methods=['POST',])
    app.add_url_rule('/asset/<int:asset_id>', view_func=asset_view, methods=['GET', 'PUT'])

    app.run(host='0.0.0.0', port=80)
