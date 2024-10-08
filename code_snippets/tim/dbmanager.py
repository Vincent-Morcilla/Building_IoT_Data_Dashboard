from pathlib import Path
import pickle
import zipfile

import pandas as pd

class DBManager:
    def __init__(self, data_zip_path, mapping_path):
        self.data_zip_path = Path(data_zip_path)
        self.mapping_path = Path(mapping_path)

        if not self.data_zip_path.is_file():
            raise FileNotFoundError(f"Data zip file not found: {self.data_zip_path}")
        
        if not self.mapping_path.is_file():
            raise FileNotFoundError(f"Mapping file not found: {self.mapping_path}")

        self.db = {}
        self.load_db()

    def __len__(self):
        return len(self.db)

    def load_db(self):
        mapping_df = pd.read_csv(self.mapping_path, index_col=0)

        # Mappings for building B only, and ignore streams not saved to file
        mapping_df = mapping_df[mapping_df['Building'] == 'B']
        mapping_df = mapping_df[mapping_df['Filename'].str.contains('FILE NOT SAVED') == False]

        with zipfile.ZipFile(self.data_zip_path, 'r') as db_zip:
            for path in db_zip.namelist():
                if not path.endswith('.pkl'):
                    continue

                filename = Path(path).name
                record = mapping_df.loc[mapping_df['Filename'] == filename, 'StreamID']

                # ignore streams that don't have a mapping
                if record.empty:
                    continue

                stream_id = record.iloc[0]

                pkl_data = db_zip.read(path)
                data = pickle.loads(pkl_data)
                data_df = pd.DataFrame(data)
                data_df.rename(columns={'t': 'time', 'v': 'value', 'y': 'label'}, inplace=True)

                self.db[stream_id] = data_df

    def get_stream(self, stream_id: str) -> pd.DataFrame:
        try:
            return self.db[str(stream_id)]
        except KeyError as exc:
            raise KeyError(f"Stream ID {stream_id} not found in the database") from exc

    def get_streams(self, stream_ids: list) -> dict:
        return {stream_id: self.get_stream(stream_id) for stream_id in stream_ids}

