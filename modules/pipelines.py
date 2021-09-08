import os
import scrapy
from itemadapter import ItemAdapter
from scrapy.exporters import JsonLinesItemExporter, CsvItemExporter

from config import name2abbrev, SAVEDIR
       
        
class PerStateJsonlinesExportPipeline:
    """Distribute items across multiple JSONL files according to their 'state_name' field"""

    def open_spider(self, spider):
        self.state_to_exporter = {}

    def close_spider(self, spider):
        for exporter, jsonl_file in self.state_to_exporter.values():
            exporter.finish_exporting()
            jsonl_file.close()

    def _exporter_for_item(self, item):
        adapter = ItemAdapter(item)
        state = name2abbrev[adapter['state_name']]
        if state not in self.state_to_exporter:
            file_path = os.path.join(SAVEDIR, f'{state}_WARN_Notices.jsonl')
            jsonl_file = open(file_path, 'ab')
            exporter = JsonLinesItemExporter(jsonl_file, encoding='utf-8')
            exporter.start_exporting()
            self.state_to_exporter[state] = (exporter, jsonl_file)
        return self.state_to_exporter[state][0]

    def process_item(self, item, spider):
        exporter = self._exporter_for_item(item)
        exporter.export_item(item)
        return item


class PerStateCsvExportPipeline:
    """Distribute items across multiple CSV files according to their 'state_name' field
    
    This creates two separate CSV files for each state, one with the raw fields and metadata,
    the other with only the normalized fields."""

    def open_spider(self, spider):
        self.state_to_exporter = {}

    def close_spider(self, spider):
        for exporter_raw, exporter_norm, csv_file_raw, csv_file_norm in self.state_to_exporter.values():
            exporter_raw.finish_exporting()
            exporter_norm.finish_exporting()
            csv_file_raw.close()
            csv_file_norm.close()

    def _exporter_for_item(self, item):
        adapter = ItemAdapter(item)
        state = name2abbrev[adapter['state_name']]
        if state not in self.state_to_exporter:
            file_path = os.path.join(SAVEDIR, f'{state}_WARN_Notices_raw.csv')
            file_path = os.path.join(SAVEDIR, f'{state}_WARN_Notices_normalized.csv')
            csv_file_raw = open(file_path, 'ab')
            csv_file_norm = open(file_path, 'ab')
            exporter_raw = CsvItemExporter(csv_file_raw, encoding='utf-8')
            exporter_norm = CsvItemExporter(csv_file_norm, encoding='utf-8')
            exporter_raw.start_exporting()
            exporter_norm.start_exporting()
            self.state_to_exporter[state] = (exporter, csv_file_norm)
        return self.state_to_exporter[state][0]

    def process_item(self, item, spider):
        exporter = self._exporter_for_item(item)
        exporter.export_item(item)
        return item


def import_from_PerStateJsonlinesExportPipeline(filepath):
    """Placeholder for re-loading exported data"""

    df = pd.DataFrame()
    return df