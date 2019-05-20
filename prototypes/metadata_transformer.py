from parse import parse, compile
from pathlib import Path
import click


@cli.command()
@click.argument('path')
@click.option('--product', help='Which product?')
@click.option('--template_path', help='Dataset yaml template file')
@click.pass_obj
def transform_metadata(path, product, template_path):
    file_paths = find_file_paths(Path(path))

    with open(str(template_path)) as fd:
        yaml_template = fd.read()

    for file_path in file_paths:
        print(file_path)
        with open(str(file_path)) as fd:
            dataset = fd.read()
        coordinates = get_coordinates(dataset)
        measurement_paths = get_measurement_paths(dataset)
        properties = get_properties(dataset)
        lineage = get_lineage(dataset)
        values = {**coordinates, **measurement_paths, **properties, **lineage}
        write_transformed_dataset(yaml_template, values)


def write_transformed_dataset(yaml_template, output_file, values):

    filled_template = yaml_template.format(**values)

    # validate and write out
    try:
        new_dataset = yaml.load(filled_template)
    except yaml.YAMLError as err:
        print(err)
    else:
        with open(output_file, 'w') as out_file:
            yaml.dump(new_dataset, out_file, default_flow_style=False)


def find_file_paths(path: Path):
    """
    Return a list of metadata yaml file path objects.
    :param path:
    :return: A generator of path objects.
    """
    for afile in path.iterdir():
        if afile.suffix == '.yaml':
            yield afile


def get_coordinates(dataset):
    """
    Extract and return geometry coordinates as a list in a dictionary:
    {
      'coordinates': [...]
    }
    """
    pass


def get_measurement_paths(dataset):
    """
    Extract and return measurement paths in a dictionary:
    {
      'coastal_aerosol': path_name1,
      'panchromatic': path_name2,
    }
    """
    pass


def get_properties(dataset, offsets=None):
    """
    Extract properties and return values in a dictionary:
    {
      'datetime': time,
      'odc_creation_datetime: creation_time
    }
    Do you want offsets in config?
    """
    pass


def get_lineage(dataset):
    """
    Extract immediate parents.
    {
      'nbar': [id1, id2]
      'pq': [id3]
    }
    """
    pass


