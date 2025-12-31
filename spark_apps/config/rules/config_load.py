import yaml
import os
import great_expectations as gx

class RuleConfigLoader:
    def __init__(self, rules_dir: str):
        self.rules_dir = rules_dir
        self.context = gx.get_context(mode="ephemeral")

    def load_suite_from_yaml(self, table_name: str) -> gx.ExpectationSuite:
        file_path = os.path.join(self.rules_dir, f"{table_name}.yaml")
        
        if not os.path.exists(file_path):
            print(f"Info: No manual config found for {table_name} at {file_path}")
            return None

        try:
            with open(file_path, 'r') as f:
                config = yaml.safe_load(f)
        except Exception as e:
            print(f"Error reading YAML: {e}")
            return None

        suite_name = f"manual_{table_name}"
        
        try:
            self.context.suites.delete(suite_name)
        except:
            pass
            
        suite = self.context.suites.add(gx.ExpectationSuite(name=suite_name))
        
        for rule in config.get("expectations", []):
            exp_type = rule.get("type") or rule.get("expectation_type")
            kwargs = rule.get("kwargs", {})
            column = rule.get("column")
            
            params = kwargs.copy()
            if column:
                params["column"] = column

            class_name = ''.join(x.title() for x in exp_type.split('_'))
            expectation_class = getattr(gx.expectations, class_name, None)
            
            if not expectation_class:
                 expectation_class = getattr(gx.expectations, exp_type, None)

            if expectation_class:
                suite.add_expectation(expectation_class(**params))
            else:
                print(f"Warning: Unknown expectation type {exp_type}")

        return suite