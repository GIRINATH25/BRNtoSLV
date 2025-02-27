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
    
    # def source_config(self):
    #     """
    #     :return: Engine
    #     """
    #     if 'source_config' in self.engines_dict:
    #         return self.engines_dict['source_config']
        
    #     engine = get_engine(self.config.get('source_config', {}))
    #     self.engines_dict['source_config'] = engine
    #     return engine

    # def source(self):
    #     """
    #     :return: Engine
    #     """
    #     if 'source' in self.engines_dict:
    #         return self.engines_dict['source']
        
    #     engine = get_engine(self.config.get('source', {}))
    #     self.engines_dict['source'] = engine
    #     return engine

    # def staging(self):
    #     """
    #     :return: Engine
    #     """
    #     if 'staging' in self.engines_dict:
    #         return self.engines_dict['staging']
        
    #     engine = get_engine(self.config.get('staging', {}))
    #     self.engines_dict['staging'] = engine
    #     return engine    

    # def dwh(self):
    #     """
    #     :return: Engine
    #     """
    #     if 'dwh' in self.engines_dict:
    #         return self.engines_dict['dwh']
        
    #     engine = get_engine(self.config.get('dwh', {}))
    #     self.engines_dict['dwh'] = engine
    #     return engine    

    # def clickhouse(self):
    #     """
    #     :return: Engine
    #     """
    #     if 'clickhouse' in self.engines_dict:
    #         return self.engines_dict['clickhouse']
        
    #     engine = get_engine(self.config.get('clickhouse', {}))
    #     self.engines_dict['clickhouse'] = engine
    #     return engine