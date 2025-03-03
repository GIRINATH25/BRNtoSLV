import yaml
from db.engine import get_engine


class DBConnector:
    
    def __init__(self) -> None:
        """
        Load the database configuration from config.yml. 
        Note config.yml should present outside of 'db' package
        """
        with open("config.yml", "r") as f:
            self.config = yaml.safe_load(f)
        
        self.engines_dict = {} 

    

    def fetch(self, data, key_name):
        if isinstance(data, dict):
            for key, value in data.items():
                if key == key_name:
                    return value
                elif isinstance(value, dict):
                    result = self.fetch(value, key_name)
                    if result is not None:
                        return result
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            result = self.fetch(item, key_name)
                            if result is not None:
                                return result


    def get_engine(self, key):
        config = self.config

        if key in self.engines_dict:
            return self.engines_dict[key]
        
        con = self.fetch(config,key)

        engine = get_engine(con)

        self.engines_dict[key] = engine

        return engine
    
    