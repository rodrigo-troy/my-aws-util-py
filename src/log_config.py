import yaml
import logging.config
import os

def configure_logging():
    try:
        directory = os.path.dirname(os.path.realpath(__file__))
        yaml_file_path = os.path.join(directory, 'log_config.yaml')
        with open(yaml_file_path, 'r') as f:
            config = yaml.safe_load(f.read())
            logging.config.dictConfig(config)
        return logging.getLogger(__name__)
    except IOError:
        print('Error: File does not exist')
    except yaml.YAMLError:
        print('Error: Invalid YAML file')
