import csv
import json
import os
from datetime import datetime, date

class Main():
    """
    Parse multiple csv's and create a unified csv
    """
    def convert_by_type(self, col_config, value):
        """
        Convert CSV value by given data type
        @param col_config -> Column info dict {type, name, format}
        @param value -> Value to be converted
        """
        dtype = col_config['type']
        if dtype == 'int':
            value = int(value)
        elif dtype == 'float':
            value = float(value)
        elif dtype == 'date':
            dt_temp = datetime.strptime(value, col_config['format'])
            value = date(dt_temp.year,
                                    dt_temp.month,
                                    dt_temp.day)
        return value


    def read_csv(self, csv_file, bank_spec):
        """
        Read csv file and create a list of data
        @param csv_file -> bank file in csv
        @param bank_spec -> Bank Spec dictionary
        """
        bank_name   = os.path.basename(csv_file).split(".")[0]
        bank_info   = bank_spec.get(bank_name)
        transform_ar = bank_info.get('transform', [])
        data        = []
        with open(csv_file) as csv_file:
            csv_data = csv.DictReader(csv_file)
            for dict_data in csv_data:
                dd = {}
                for field in bank_info['fields']:
                    name    = field['name']
                    value   = dict_data[name]
                    dd["bank_name"] = bank_name
                    dd[name]  = self.convert_by_type(field, value)
                self.transform(dd, transform_ar) 
                data.append(dd)
        return (bank_name, data)


    def to_csv_file(self, data, csv_spec, output_file):
        """
        Write converted data to csv file
        """
        with open(output_file, 'w', newline='') as csv_file:
            header = []
            bank_name = list(csv_spec.keys())[0]
            for field in csv_spec[bank_name]["to_csv"]:
                header.append(field['name'])
            csv_output = csv.writer(csv_file)

            csv_output.writerow(header)

            for bank_name, bank_data in data:
                for dd in bank_data:
                    row_ar = []
                    for field in csv_spec[bank_name]["to_csv"]:
                        row_ar.append(dd[field['field']])
                    csv_output.writerow(row_ar)


    
    def transform(self, data_dict, transform_ar):
        """
        Transform the data to another value
        @param data_dict-> Row data dict
        @param transform_to - > tranform filed list
        """
        for rule in transform_ar:
            name = rule[1]
            if rule[0] == 'add':
                data_dict[name] = data_dict[name] + rule[2]
            elif rule[0] == 'add_fields':
                data_dict[name] = data_dict[name] + data_dict[rule[2]]
            elif rule[0] == 'divide':
                data_dict[name] = data_dict[name] / rule[2]
            elif rule[0] == 'multiply':
                data_dict[name] = data_dict[name] * rule[2]
            elif rule[0] == 'subtract':
                data_dict[name] = data_dict[name] - rule[2]

    def get_all_csv_file(self, dirname):
        """
        Get all csv files
        @param dirname -> file localtion
        :return -> List of csv filpath
        """
        files   = []
        for root, dirnames, filenames in os.walk(dirname):
            for f in filenames:
                if f.split('.')[-1] == 'csv':
                    files.append(os.path.join(root, f))
        return files


    def load_json(self, json_file):
        with open(json_file) as f:
            data = json.load(f)

        return data

    def create_unified_csv(self, dirname):
        """
        Parse multiple csv's and create a unified csv
        @param dirname -> directory which contains csv files
        """
        files       = self.get_all_csv_file(dirname)
        bank_spec   = self.load_json("bank_spec.json")
        data        = []
        for csv_file in files:
            data.append(self.read_csv(csv_file, bank_spec))
        self.to_csv_file(data, bank_spec, "unified_csv.csv")

if __name__ == '__main__':
    obj = Main()
    obj.create_unified_csv('data')