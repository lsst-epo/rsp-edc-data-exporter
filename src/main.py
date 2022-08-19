import os, fnmatch, json, subprocess, csv, shutil, time
import glob # for debugging
from citizen_science_validator import CitizenScienceValidator
from data_exporter_response import DataExporterResponse
from flask import Flask, request, Response
from google.cloud import storage
import panoptes_client
from panoptes_client import Panoptes, Project, SubjectSet
import sqlalchemy
from sqlalchemy import select, update
import numpy as np
import threading
# Imports the Cloud Logging client library
from google.cloud import logging
# import lsst.daf.butler as dafButler
from models.citizen_science_batches import CitizenScienceBatches
from models.citizen_science_projects import CitizenScienceProjects
from models.citizen_science_owners import CitizenScienceOwners
from models.citizen_science_meta import CitizenScienceMeta
from models.citizen_science_proj_meta_lookup import CitizenScienceProjMetaLookup

app = Flask(__name__)

CLOUD_STORAGE_BUCKET = os.environ['CLOUD_STORAGE_BUCKET']
CLOUD_STORAGE_BUCKET_HIPS2FITS = os.environ['CLOUD_STORAGE_BUCKET_HIPS2FITS']
CLOUD_STORAGE_CIT_SCI_PUBLIC = os.environ["CLOUD_STORAGE_CIT_SCI_PUBLIC"]
DB_USER = os.environ['DB_USER']
DB_PASS = os.environ['DB_PASS']
DB_NAME = os.environ['DB_NAME']
DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ['DB_PORT']
db = None
CLOSED_PROJECT_STATUSES = ["COMPLETE", "CANCELLED", "ABANDONED"]
BAD_OWNER_STATUSES = ["BLOCKED", "DISABLED"]

# Instantiates the logging client
logging_client = logging.Client()
log_name = "rsp-data-exporter"
logger = logging_client.logger(log_name)
response = DataExporterResponse()
validator = CitizenScienceValidator()
debug = False
urls = []

@app.route("/citizen-science-ingest-status")
def check_status_of_previously_executed_ingest():
    global response
    guid = request.args.get("guid")

    gcs = storage.Client()

    manifest_path = guid + "/manifest.csv"

    # Get the bucket that the file will be uploaded to.
    bucket = gcs.bucket(CLOUD_STORAGE_CIT_SCI_PUBLIC)
    exists = storage.Blob(bucket=bucket, name=manifest_path).exists(gcs)

    response = DataExporterResponse()
    response.messages = []

    if exists:
        response.status = "success"
        response.manifest_url = "https://storage.googleapis.com/citizen-science-data/" + guid + "/manifest.csv"
    else:
        response.status = "error"
        response.messages.append("The job either failed or is still processing, please try again later.")

    res = json.dumps(response.__dict__)
    return res

@app.route("/citizen-science-bucket-ingest")
def download_bucket_data_and_process():
    global response, validator, debug, urls
    guid = request.args.get("guid")
    email = request.args.get("email")
    vendor_project_id = request.args.get("vendor_project_id")
    vendor_batch_id = request.args.get("vendor_batch_id")
    debug = bool(request.args.get("debug"))
    # large_import = bool(request.args.get("large_import"))
    response = DataExporterResponse()
    response.messages = []
    validator = CitizenScienceValidator()
    urls = []

    time_mark(debug, __name__)
    
    validate_project_metadata(email, vendor_project_id, vendor_batch_id)

    if validator.error is False:
        cutouts = download_zip(CLOUD_STORAGE_BUCKET_HIPS2FITS, guid + ".zip", guid)
    
        if validator.error is False:
            urls = upload_cutouts(cutouts, vendor_project_id)

            if validator.error is False:
            
                manifest_url = build_and_upload_manifest(urls, email, "556677", CLOUD_STORAGE_CIT_SCI_PUBLIC, guid + "/")
            
                response.status = "success"
                response.manifest_url = manifest_url
    else:
        response.status = "error"
        if response.messages == None or len(response.messages) == 0:
            response.messages.append("An error occurred while processing the data batch, please try again later.")

    res = json.dumps(response.__dict__)
    # logger.log_text(res)
    time_mark(debug, "Done processing, return response to notebook aspect")
    return res

# Note: plural
def upload_cutouts(cutouts, vendor_project_id):
    global debug, urls

    # Beginning of optimization code
    time_mark(debug, "Start of upload...")
    if len(cutouts) > 500: # Arbitrary threshold for threading
        logger.log_text("inside of the upload_cutouts len(cutouts) IF code block")
        subset_count = round(len(np.array(cutouts)) / 250)
        logger.log_text("subset_count: " + str(subset_count))
        sub_cutouts_arr = np.split(np.array(cutouts), subset_count) # create sub arrays divided by 1k cutouts
        threads = []
        for i, sub_arr in enumerate(sub_cutouts_arr):
            logger.log_text("i : " + str(i));
            t = threading.Thread(target=upload_cutout_arr, args=(sub_arr,str(i),))
            threads.append(t)
            logger.log_text("starting thread #" + str(i))
            threads[i].start()
        
        for thread in threads:
            logger.log_text("joining thread!")
            thread.join()

    else:
        upload_cutout_arr(cutouts, str(1))
    time_mark(debug, "End of upload...")

    time_mark(debug, "Start of upload & inserting of metadata...")
    insert_meta_records(urls, vendor_project_id)
    time_mark(debug, "End of inserting of metadata records")
    return urls

# Note: singular 
def upload_cutout_arr(cutouts, i):
    global urls
    gcs = storage.Client()
    bucket = gcs.bucket(CLOUD_STORAGE_CIT_SCI_PUBLIC)

    already_logged = False

    for cutout in cutouts:
        if already_logged == False:
            logger.log_text(cutout)
            already_logged = True
        destination_filename = cutout.replace("/tmp/", "")
        blob = bucket.blob(destination_filename)
        
        blob.upload_from_filename(cutout)
        urls.append(blob.public_url)

    logger.log_text("finished uploading thread #" + i)

    return
        
def insert_meta_records(urls, vendor_project_id):
    time_mark(debug, "Start of metadata record insertion")
    for url in urls:
        insert_meta_record(url, str(round(time.time() * 1000)) , 'sourceId', vendor_project_id)
    time_mark(debug, "End of metadata record insertion")
    return

# Accepts the bucket name and filename to download and returns the path of the downloaded file
def download_zip(bucket_name, filename, file = None):
    global response, validator, db, debug
    time_mark(debug, "Start of download zip")
    # Create a Cloud Storage client.
    gcs = storage.Client()

    # Get the bucket that the file will be uploaded to.
    bucket = gcs.bucket(bucket_name)

    # Download the file to /tmp storage
    blob = bucket.blob(filename)
    zipped_cutouts = "/tmp/" + filename
    time_mark(debug, "Start of download...")
    blob.download_to_filename(zipped_cutouts)
    time_mark(debug, "Download finished...")

    # logger.log_text("rosas - about to log the /tmp directory contents")
    # rosas_test = str(glob.glob("/tmp/*"))
    # logger.log_text(rosas_test)

    unzipped_cutouts_dir = "/tmp/" + file
    os.mkdir(unzipped_cutouts_dir)
    time_mark(debug, "Start of unzip....")
    shutil.unpack_archive(zipped_cutouts, unzipped_cutouts_dir, "zip")
    time_mark(debug, "Unzip finished...")

    # Count the number of objects and remove any files more than the allotted amount based on
    # the RSP user's data rights approval status
    time_mark(debug, "Start of dir count...")
    files = os.listdir(unzipped_cutouts_dir)
    time_mark(debug, "Dir count finished...")
    max_objects_count = 100
    if validator.data_rights_approved == True:
        max_objects_count = 10000
    else:
        response.messages.append("Your project has not been approved by the data rights panel as of yet, as such you will not be able to send any additional data to Zooniverse until your project is approved.")

    if len(files) > max_objects_count:
        response.messages.append("Currently, a maximum of " + str(max_objects_count) + " objects is allowed per batch for your project - your batch has been has been truncated and anything in excess of " + str(max_objects_count) + " objects has been removed.")
        time_mark(debug, "Start of truncating excess files")
        for f_file in files[max_objects_count:]:
            # response.messages.append("Removing file : " + unzipped_cutouts_dir + "/" + f_file)
            os.remove(unzipped_cutouts_dir + "/" + f_file)
        time_mark(debug, "Truncating finished...")

    # Now, limit the files sent to image files
    time_mark(debug, "Start of grabbing all the cutouts for return...")
    pngs = glob.glob("/tmp/" + file + "/*.png")
    jpegs = glob.glob("/tmp/" + file + "/*.jpeg")
    jpgs = glob.glob("/tmp/" + file + "/*.jpg")
    cutouts = pngs + jpegs + jpgs
    time_mark(debug, "Grabbing cutouts finished...")
    return cutouts

def build_and_upload_manifest(urls, email, sourceId, bucket, destination_root = ""):
    global debug
    time_mark(debug, "In build and upload manifest")
    # Create a Cloud Storage client.
    gcs = storage.Client()

    # Get the bucket that the file will be uploaded to.
    bucket = gcs.bucket(bucket)

    # loop over urls
    with open('/tmp/manifest.csv', 'w', newline='') as csvfile:
        fieldnames = ['email', 'location:1', 'external_id']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        offset = 1
        for url in urls:
            writer.writerow({'email': email, 'location:1': url, 'external_id': str(round(time.time() * 1000) + offset) })
            offset += 1
    
    manifestBlob = bucket.blob(destination_root + "manifest.csv")

    manifestBlob.upload_from_filename("/tmp/manifest.csv")
    return manifestBlob.public_url

def validate_project_metadata(email, vendor_project_id, vendor_batch_id = None):
    global validator, debug, response
    newOwner = False

    # Lookup if owner record exists, if so then return it
    ownerId = lookup_owner_record(email)

    # Create owner record
    if validator.error == False:
        if ownerId == None:
            newOwner = True
            ownerId = create_new_owner_record(email)
    else:
        return
        
    # Then, lookup project
    if validator.error == False:
        if(newOwner == True):
            project_id = create_new_project_record(vendor_project_id)
        else:
            project_id = lookup_project_record(vendor_project_id)
            if project_id is None:
                project_id = create_new_project_record(ownerId, vendor_project_id)
    else:
        return

    # Then, check batch status
    if validator.error == False:
        batch_id = check_batch_status(project_id, vendor_project_id) # To-do: Look into whether or not this would be a good time to check with Zoony on the status of the batches

        if batch_id < 0:
            # Create new batch record
            batchId = create_new_batch(project_id, vendor_batch_id)

            if(batchId > 0):
                return True
            else:
                return False
        else:
            validator.error = True
            response.messages.append("You currently have an active batch of data on the Zooniverse platform and cannot create a new batch until the current batch has been completed.")
            return False
    else:
        return False

# To-do: Add vendor_batch_id to workflow of this function/route
@app.route("/citizen-science-butler-ingest")
def butler_retrieve_data_and_upload():
    global db
    db = init_connection_engine()
    email = request.args.get('email')
    collection = request.args.get('collection')
    source_id = request.args.get("sourceId")
    vendor_project_id = request.args.get("vendorProjectId")
    output = subprocess.run(['sh', '/opt/lsst/software/server/run.sh', email, collection], stdout=subprocess.PIPE).stdout.decode('utf-8')

    # Create a Cloud Storage client.
    gcs = storage.Client()

    # Get the bucket that the file will be uploaded to.
    bucket = gcs.bucket(CLOUD_STORAGE_BUCKET)

    filepath = locate('*.fits', "/tmp/data/" + collection)
    # # Create a new blob and upload the file's content.
    blob = bucket.blob(filepath[1])

    blob.upload_from_filename(str(filepath[0]))

    manifest_url = build_and_upload_manifest(['https://storage.googleapis.com/butler-config/astrocat.jpeg'], email, "000222444", CLOUD_STORAGE_BUCKET)
    valid_project_metadata = validate_project_metadata(email, vendor_project_id)

    if valid_project_metadata is True:
        # Finally, insert meta records
        insert_meta_record(manifest_url, source_id, 'sourceId', vendor_project_id)
    return manifest_url
        
def create_new_batch(project_id, vendor_batch_id):
    global validator, response, debug
    time_mark(debug, "Start of create new batch")
    batchId = -1;
    try:
        db = CitizenScienceBatches.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
        citizen_science_batch_record = CitizenScienceBatches(cit_sci_proj_id=project_id, vendor_batch_id=vendor_batch_id, batch_status='ACTIVE')    
        db.add(citizen_science_batch_record)
        db.commit()
        logger.log_text("about to log citizen_science_batch_record")
        logger.log_text(dir(citizen_science_batch_record))
        logger.log_text("about to log citizen_science_batch_record.cit_sci_batch_id")
        logger.log_text(citizen_science_batch_record.cit_sci_batch_id)
        batchId = citizen_science_batch_record.cit_sci_batch_id
    except Exception as e:
        logger.log_text(e)
        validator.error = True
        response.status = "error"
        response.messages.append("An error occurred while attempting to create a new data batch record for you - this is usually due to an internal issue that we have been alerted to. Apologies about the downtime - please try again later.")

    return batchId

def check_batch_status(project_id, vendor_project_id):
    # First, look up batches in the database, which may 
    global validator, debug
    time_mark(debug, "Start of check batch status")
    batch_id = -1
    vendor_batch_id_db = 0
    try:
        db = CitizenScienceBatches.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
        stmt = select(CitizenScienceBatches).where(CitizenScienceBatches.cit_sci_proj_id == project_id).where(CitizenScienceBatches.batch_status == 'ACTIVE')
        results = db.execute(stmt)
        for row in results.scalars():
            batch_id = row['cit_sci_batch_id']
            vendor_batch_id_db = row['vendor_batch_id']
            break
        logger.log_text("about to log batch_id")
        logger.log_text(batch_id)
        logger.log_text("about to log vendor_batch_id_db")
        logger.log_text(vendor_batch_id_db)
    except Exception as e:
        logger.log_text(e)
        validator.error = True
        response.status = "error"
        response.messages.append("An error occurred while attempting to lookup your batch records - this is usually due to an internal issue that we have been alerted to. Apologies about the downtime - please try again later.")
        return

    if batch_id > 0: # An active batch record was found in the DB
        # Call the Zooniverse API to get all subject sets for the project
        project = Project.find(int(vendor_project_id))

        update_batch_record = False;
        for sub in list(project.links.subject_sets):
            if str(vendor_batch_id_db) == sub.id:
                for completeness_key in sub.completeness:
                    if sub.completeness[completeness_key] == 1.0:
                        update_batch_record = True
                    else:
                        update_batch_record = False
                        break
        if update_batch_record == True:
            try:
                logger.log_text("about to update batch in batch_id > 0 IF code block")
                updt_stmt = update(CitizenScienceBatches).values(batch_status = "COMPLETE").where(CitizenScienceBatches.cit_sci_proj_id == project_id).where(CitizenScienceBatches.cit_sci_batch_id == batch_id)
                db.execute(updt_stmt)
            except Exception as e:
                logger.log_text(e)
                validator.error = True
                response.status = "error"
                response.messages.append("An error occurred while attempting to create a new data batch record for you - this is usually due to an internal issue that we have been alerted to. Apologies about the downtime - please try again later.")
    
    return batch_id

def create_new_project_record(ownerId, vendorProjectId):
    global validator, response, debug
    time_mark(debug, "Start of create new project")
    project_id = None
    try:
        db = CitizenScienceProjects.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
        citizen_science_project_record = CitizenScienceBatches(vendor_project_id=vendorProjectId, project_status='ACTIVE')
        db.add(citizen_science_project_record)
        db.commit()
        project_id = citizen_science_project_record.cit_sci_proj_id

        logger.log_text("about to log project_id")
        logger.log_text(project_id)

    except Exception as e:
        validator.error = True
        response.status = "error"
        response.messages.append("An error occurred while attempting to create a new project owner record for you - this is usually due to an internal issue that we have been alerted to. Apologies about the downtime - please try again later.")

    return project_id

def lookup_project_record(vendorProjectId):
    global response, validator, debug
    time_mark(debug, "Start of lookup project record")
    project_id = None

    try:
        db = CitizenScienceProjects.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
        stmt = select(CitizenScienceProjects).where(CitizenScienceProjects.vendor_project_id == vendorProjectId)

        results = db.execute(stmt)
        for row in results.scalars():
            status = row['project_status']
            validator.data_rights_approved = row["data_rights_approved"]

            if status in CLOSED_PROJECT_STATUSES:
                response.status = "error"
                validator.error = True
                response.messages.append("This project is in a status of " + status + " - either create a new project or contact Rubin to request for the project to be reopened.")
            else:
                project_id = row['cit_sci_proj_id']

        logger.log_text("about to log status in lookup_project_record()")
        logger.log_text(status)
        logger.log_text("logging validator.data_rights_approved")
        logger.log_text(validator.data_rights_approved)

    except Exception as e:
        validator.error = True
        response.status = "error"
        response.messages.append("An error occurred while attempting to lookup your project record - this is usually due to an internal issue that we have been alerted to. Apologies about the downtime - please try again later.")

    return project_id

def create_new_owner_record(email):
    global validator, response, debug
    time_mark(debug, "Start of create new owner")

    owner_id = None;
    try:
        db = CitizenScienceOwners.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
        citizen_science_owner_record = CitizenScienceOwners(email=email, status='ACTIVE')
        db.add(citizen_science_owner_record)
        db.commit()
        owner_id = citizen_science_owner_record.cit_sci_owner_id
        # return owner_id

    except Exception as e:
        validator.error = True
        response.status = "error"
        response.messages.append("An error occurred while attempting to create a new project owner record for you - this is usually due to an internal issue that we have been alerted to. Apologies about the downtime - please try again later.")

    return owner_id

def lookup_owner_record(emailP):
    global validator, response, debug
    time_mark(debug, "Looking up owner record")
    # stmt = sqlalchemy.text(
    #     "SELECT cit_sci_owner_id, status FROM citizen_science_owners WHERE email=:email"
    # )
    ownerId = None
    status = ""

    try:
        db = CitizenScienceOwners.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
        stmt = select(CitizenScienceOwners).where(CitizenScienceOwners.email == emailP)

        results = db.execute(stmt)
        for row in results.scalars():
            ownerId = row['cit_sci_owner_id']
            status = row['status']

            if status in BAD_OWNER_STATUSES:
                validator.error = True
                response.status = "error"
                response.messages.append("You are not/no longer eligible to use the Rubin Science Platform to send data to Zooniverse.")

                # return ownerId

    except Exception as e:
        validator.error = True
        response.status = "error"
        response.messages.append("An error occurred while looking up your projects owner record - this is usually due to an internal issue that we have been alerted to. Apologies about the downtime - please try again later.")
   
    return ownerId

def lookup_meta_record(sourceId, sourceIdType):
    try:
        db = CitizenScienceMeta.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
        stmt = select(CitizenScienceMeta).where(CitizenScienceProjects.source_id == sourceId).where(CitizenScienceProjects.source_id_type == sourceIdType)
        results = db.execute(stmt)
        for row in results.scalars():
            metaId = row.cit_sci_meta_id

        logger.log_text("about to log metaId in lookup_meta_record()")
        logger.log_text(metaId)

    except Exception as e:
        print(e)
        return e
   
    return metaId

def insert_meta_record(uri, sourceId, sourceIdType, projectId):
    global debug

    # Temp hardcoded variables
    edcVerId = 11000222
    public = True

    errorOccurred = False;
    try:
        db = CitizenScienceMeta.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
        citizen_science_meta_record = CitizenScienceMeta(edc_ver_id=edcVerId, source_id=sourceId, source_id_type=sourceIdType, uri=uri, public=public)
        db.add(citizen_science_meta_record)
        db.commit()
        metaRecordId = citizen_science_meta_record.cit_sci_meta_id
        errorOccurred = True if insert_lookup_record(metaRecordId, projectId) else False

        logger.log_text("about to log metaRecordId")
        logger.log_text(metaRecordId)

    except Exception as e:
        # Is the exception because of a duplicate key error? If so, lookup the ID of the meta record and perform the insert into the lookup table
        if "non_dup_records" in e.__str__():
            metaId = lookup_meta_record(sourceId, sourceIdType)
            return insert_lookup_record(metaId, projectId)
        return False
    return errorOccurred

def insert_lookup_record(metaRecordId, projectId):
    try:
        db = CitizenScienceProjMetaLookup.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
        citizen_science_proj_meta_lookup_record = CitizenScienceProjMetaLookup(cit_sci_proj_id=projectId, cit_sci_meta_id=metaRecordId)
        db.add(citizen_science_proj_meta_lookup_record)
        db.commit()
            
    except Exception as e:
        logger.exception(e.__str__())
        return False
        
    return True

def locate(pattern, root_path):
    for path, dirs, files in os.walk(os.path.abspath(root_path)):
        for filename in fnmatch.filter(files, pattern):
            return [os.path.join(path, filename), filename ]

def init_connection_engine():
    db_config = {
        # [START cloud_sql_postgres_sqlalchemy_limit]
        # Pool size is the maximum number of permanent connections to keep.
        "pool_size": 5,
        # Temporarily exceeds the set pool_size if no connections are available.
        "max_overflow": 2,
        # The total number of concurrent connections for your application will be
        # a total of pool_size and max_overflow.
        # [END cloud_sql_postgres_sqlalchemy_limit]

        # [START cloud_sql_postgres_sqlalchemy_backoff]
        # SQLAlchemy automatically uses delays between failed connection attempts,
        # but provides no arguments for configuration.
        # [END cloud_sql_postgres_sqlalchemy_backoff]

        # [START cloud_sql_postgres_sqlalchemy_timeout]
        # 'pool_timeout' is the maximum number of seconds to wait when retrieving a
        # new connection from the pool. After the specified amount of time, an
        # exception will be thrown.
        "pool_timeout": 30,  # 30 seconds
        # [END cloud_sql_postgres_sqlalchemy_timeout]

        # [START cloud_sql_postgres_sqlalchemy_lifetime]
        # 'pool_recycle' is the maximum number of seconds a connection can persist.
        # Connections that live longer than the specified amount of time will be
        # reestablished
        "pool_recycle": 1800,  # 30 minutes
        # [END cloud_sql_postgres_sqlalchemy_lifetime]
    }


    return init_tcp_connection_engine(db_config)

def init_tcp_connection_engine(db_config):
    pool = sqlalchemy.create_engine("postgresql://{}:{}@{}:{}/{}".format(DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME))
    pool.dialect.description_encoding = None
    return pool

def time_mark(debug, milestone):
    if debug == True:
        logger.log_text("Time mark - " + str(round(time.time() * 1000)) + " - in " + milestone);

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))