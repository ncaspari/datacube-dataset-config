# coding: utf-8
import logging
import re
import uuid
from multiprocessing import Process, current_process, Manager, cpu_count
from queue import Empty

import os
import boto3
import click
import xml.etree.ElementTree as ET
import json

import datacube
from datacube.index.hl import Doc2Dataset
from datacube.utils import changes

#### THESE NEED TO BE ADAPTED ##################################################

# Tuple of product band name, resolution and band names:
# 1. name of the band, as defined in product description yaml
# 2. resolution (as in /tiles/32/A/BC/2020/2/21/0/{resolution}/{band}.jp2)
# 3. band (as in /tiles/32/A/BC/2020/2/21/0/{resolution}/{band}.jp2)

bands_of_interest = [
('B02_10m', 'R10m', 'B02'),
('B03_10m', 'R10m', 'B03'),
('B04_10m', 'R10m', 'B04'),
('B08_10m', 'R10m', 'B08'),
('B02_20m', 'R20m', 'B02'),
('B03_20m', 'R20m', 'B03'),
('B04_20m', 'R20m', 'B04'),
('B05_20m', 'R20m', 'B05'),
('B06_20m', 'R20m', 'B06'),
('B07_20m', 'R20m', 'B07'),
('B8A_20m', 'R20m', 'B8A'),
('B11_20m', 'R20m', 'B11'),
('B12_20m', 'R20m', 'B12'),
('SCL_20m', 'R20m', 'SCL'),
('B01_60m', 'R60m', 'B01'),
('B02_60m', 'R60m', 'B02'),
('B03_60m', 'R60m', 'B03'),
('B04_60m', 'R60m', 'B04'),
('B05_60m', 'R60m', 'B05'),
('B06_60m', 'R60m', 'B06'),
('B07_60m', 'R60m', 'B07'),
('B8A_60m', 'R60m', 'B8A'),
('B09_60m', 'R60m', 'B09'),
('B11_60m', 'R60m', 'B11'),
('B12_60m', 'R60m', 'B12'),

]

################################################################################

IMAGE_FILE_ENDINGS = '.jp2'
PRODUCT_META_FILE_NAME = 'metadata.xml'
TILE_META_FILE_NAME = 'tileInfo.json'
GUARDIAN = "GUARDIAN_QUEUE_EMPTY"

################################################################################

def get_extent_coords(ext_pos_list_string):
    """Rertrieve the coordinates of the tile's footprint
    
    :param ext_pos_list_string: Raw string from granule's XML element 'EXT_POS_LIST'
    :type ext_pos_list_string: str
    :return: Dictionary of boundary coordinates
    :rtype: dict[str:str]
    """
    ext_pos_list_string = ext_pos_list_string.split(' ')
    return {
        'ul': {'lat': ext_pos_list_string[0], 'lon':  ext_pos_list_string[1]},
        'ur': {'lat': ext_pos_list_string[2], 'lon':  ext_pos_list_string[3]},
        'lr': {'lat': ext_pos_list_string[4], 'lon':  ext_pos_list_string[5]},
        'll': {'lat': ext_pos_list_string[6], 'lon':  ext_pos_list_string[7]},
    }


def generate_band_paths(bucket_name, tile_base_path, bands_of_interest, 
                        image_file_ending):
                    
    band_dict = {}
    band_path = 's3://{bucket_name}/{tile_base_path}/{resolution}/{band}{image_file_ending}'

    for name, resolution, band in bands_of_interest:
        band_dict[name] = {
            'layer': 1,
            'path': band_path.format(bucket_name=bucket_name, band=band, 
                                     tile_base_path=tile_base_path,
                                     resolution=resolution, 
                                     image_file_ending=image_file_ending)
        }
    return band_dict


def get_s3_url(bucket_name, obj_key):
    return 's3://{bucket_name}/{obj_key}'.format(
        bucket_name=bucket_name, obj_key=obj_key)


def extract_product_meta(raw_product_meta_file, bucket_name, object_key):
    """Extract product metadata
    
    :param raw_product_meta_file: File stream for product metadata file
    :type raw_product_meta_file: botocore.response.StreamingBody
    :param bucket_name: bucket name
    :type bucket_name: str
    :param object_key: S3 object key for product metadata file
    :type object_key: str
    :return: Dictionary with datacube releavnt product metadata
    :rtype: dict
    """
    tree = ET.parse(raw_product_meta_file)
    root = tree.getroot()

    level = root.findall('.//Product_Info/PROCESSING_LEVEL')[0].text
    product_type = root.findall('.//Product_Info/PRODUCT_TYPE')[0].text
    creation_dt = root.findall('.//Product_Info/GENERATION_TIME')[0].text
    sensing_dt = root.findall('.//Product_Info/PRODUCT_START_TIME')[0].text
    satellite = root.findall('.//SPACECRAFT_NAME')[0].text
    instrument = 'MSI'
    coordinates = get_extent_coords(root.findall(
        './/Product_Footprint/Product_Footprint/Global_Footprint/EXT_POS_LIST'
        )[0].text)
    image_format =  root.findall('.//Granule_List/')[0].attrib['imageFormat']

    dataset_doc_part = {
        'id': str(uuid.uuid5(uuid.NAMESPACE_URL, 
                             get_s3_url(bucket_name, object_key))),
        'processing_level': level,
        'product_type': product_type,
        'creation_dt': creation_dt,
        'platform': {'code': satellite},
        'instrument': {'name': instrument},
        'extent': {
            'from_dt': sensing_dt,
            'to_dt': sensing_dt,
            'center_dt': sensing_dt,
            'coord': coordinates,
        },
        'format': {'name': image_format},
        'lineage': {'source_datasets': {}},
    }
    return dataset_doc_part


def extract_tile_meta(raw_tile_meta_file):
    """Extract product definition path, CRS and geo reference points from 
    given tile metadata file
    
    :param tile_meta_path: Path to tile metadata file
    :type tile_meta_path: str
    :return: Dictionary with CRS and dictionary of geo reference points.
    :rtype: dict['crs_epsg': str, 'geo_ref_points': dict[str:int]]
    """    
    tile_meta = json.loads(raw_tile_meta_file)

    product_path = tile_meta['productPath']

    crs_epsg = tile_meta['tileGeometry']['crs']['properties']['name'].split(':')[-1]
    crs_epsg = 'EPSG:' + crs_epsg

    ul = tile_meta['tileGeometry']['coordinates'][0][0]
    ur = tile_meta['tileGeometry']['coordinates'][0][1]
    lr = tile_meta['tileGeometry']['coordinates'][0][2]
    ll = tile_meta['tileGeometry']['coordinates'][0][3]

    geo_ref_points = {
        'ul': {'x': ul[0], 'y': ul[1]},
        'ur': {'x': ur[0], 'y': ur[1]},
        'lr': {'x': lr[0], 'y': lr[1]},
        'll': {'x': ll[0], 'y': ll[1]},
    }

    return({'product_path': product_path, 'crs_epsg': crs_epsg, 
            'geo_ref_points': geo_ref_points})



def archive_document(doc, uri, index, sources_policy):
    """Archive dataset
    
    :param doc: Dict of parameters that reference the dataset
    :type doc: dict
    :param uri: URI to metadata file for the tile
    :type uri: str
    :param index: Datacube index
    :type index: datacube.index
    :param sources_policy: Source policy  
    :type sources_policy: str
    """
    def get_ids(dataset):
        ds = index.datasets.get(dataset.id, include_sources=True)
        for source in ds.sources.values():
            yield source.id
        yield dataset.id

    resolver = Doc2Dataset(index)
    dataset, err = resolver(doc, uri)
    index.datasets.archive(get_ids(dataset))
    logging.info("Archiving %s and all sources of %s", dataset.id, dataset.id)


def add_dataset(doc, uri, index, sources_policy):
    """Add dataset documentation to datacube
    
    :param doc: Dict of parameters to index
    :type doc: dict
    :param uri: URI to metadata file for the tile
    :type uri: str
    :param index: Datacube index
    :type index: datacube.index
    :param sources_policy: Source policy  
    :type sources_policy: str
    :return: dataset or error
    :rtype: dataset or error
    """
    
    logging.info("Adding %s to index", uri)

    resolver = Doc2Dataset(index)
    dataset, err = resolver(doc, uri)

    if err is not None:
        logging.error("%s", err)
    else:
        try:
            index.datasets.add(dataset, sources_policy=sources_policy)
        except changes.DocumentMismatchError as e:
            index.datasets.update(dataset, {tuple(): changes.allow_any})
        except Exception as e:
            err = e
            logging.error("Unhandled exception %s", e)

    return dataset, err



def worker(config, bucket_name, prefix, func, sources_policy, queue, request_payer):
    dc = datacube.Datacube(config=config)
    index = dc.index
    s3 = boto3.resource("s3")

    while True:
        try:
            key = queue.get(timeout=60)
            if key == GUARDIAN:
                break

            logging.info("Processing %s %s", key, current_process())

            tile_base_path = '/'.join(key.split('/')[:-1]) 

            tile_meta_obj = s3.Object(bucket_name, key)\
                .get(ResponseCacheControl='no-cache', RequestPayer=request_payer)
            raw_tile_meta_file = tile_meta_obj['Body'].read()
            tile_meta = extract_tile_meta(raw_tile_meta_file)

            spatial_doc_part = {
                'grid_spatial': {
                    'projection': {
                        'geo_ref_points': tile_meta['geo_ref_points'],
                        'spatial_reference': tile_meta['crs_epsg']
                    }
                }
            }

            product_meta_path = tile_meta['product_path'] + '/' + PRODUCT_META_FILE_NAME
            product_meta_obj = s3.Object(bucket_name, product_meta_path)\
                .get(ResponseCacheControl='no-cache', RequestPayer=request_payer)
            raw_product_meta_file = product_meta_obj['Body']

            product_doc_part = extract_product_meta(raw_product_meta_file, 
                                                bucket_name, 
                                                product_meta_path)
            
            bands_paths = generate_band_paths(bucket_name, tile_base_path, 
                                        bands_of_interest, IMAGE_FILE_ENDINGS)

            image_doc_part = {'image': {'bands': bands_paths}}

            dataset_doc = {**product_doc_part, **spatial_doc_part, ** image_doc_part}

            uri = get_s3_url(bucket_name, key)
            func(dataset_doc, uri, index, sources_policy)
            queue.task_done()
        
        except Empty:
            break
        except EOFError:
            break


def iterate_datasets(bucket_name, config, prefix, func, 
                     sources_policy, request_payer):
    manager = Manager()
    queue = manager.Queue()

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    logging.info("Starting indexing for: bucket: '%s', with key prefix: '%s'", 
                 bucket_name, str(prefix))
    worker_count = cpu_count() # * 2

    processess = []
    for i in range(worker_count):
        proc = Process(target=worker, args=(config, bucket_name, prefix, 
                                            func, sources_policy, queue,
                                            request_payer))
        processess.append(proc)
        proc.start()

    for obj in bucket.objects.filter(Prefix=str(prefix), 
                                     RequestPayer=request_payer):
        if obj.key.endswith(TILE_META_FILE_NAME):
            queue.put(obj.key)

    for i in range(worker_count):
        queue.put(GUARDIAN)

    for proc in processess:
        proc.join()


@click.command(help="Enter Bucket name. Optional to enter configuration file to\
                     access a different ODC database.")
@click.argument('bucket_name')
@click.option('--config', '-c', help=" Pass the configuration file to access the database",
              type=click.Path(exists=True))
@click.option('--prefix', '-p', help="Pass the prefix of the object to the bucket")
@click.option('--archive', is_flag=True,
              help="If true, datasets found in the specified bucket and\
                    prefix will be archived")
@click.option('--sources_policy', default="verify", help="verify, ensure, skip")
@click.option('--requester_pays', is_flag=True,
              help="Needs to be passed when indexing requester-pays S3 buckets\
                    (e.g. arn:aws:s3:::sentinel-s2-l2a)")
def main(bucket_name, config, prefix, archive, sources_policy, requester_pays):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
    action = archive_document if archive else add_dataset
    request_payer = 'requester' if requester_pays else 'owner'
    iterate_datasets(bucket_name, config, prefix, action, sources_policy, request_payer)


if __name__ == "__main__":
    main()