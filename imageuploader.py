# import firebase_admin
# from firebase_admin import credentials
# from firebase_admin import firestore
from google.cloud import storage


def upload_images_towaven(file_name):
    storage_client = storage.Client.from_service_account_json('firestore-key.json')

    buckets = list(storage_client.list_buckets())
    bucket = storage_client.get_bucket("waven-backend.appspot.com")

    blob = bucket.blob('prod-imgs/{}'.format(file_name))
    url_link = blob.upload_from_filename('./{}'.format(file_name))
    # print(buckets)
    print("[+] Uploaded: {}".format(blob.public_url))


upload_images_towaven('vassago.jpg')


