from io import BytesIO
import os
from zipfile import ZipFile
import argparse

from tqdm import tqdm

from PIL import Image
import pandas as pd

import ifcb

DEFAULT_HOST = 'ifcb-data.whoi.edu'
DEFAULT_DATASET = 'mvco'

OUTPUT_DIR = './output'

def main(host, dataset, bin, output_dir):
    url = f'https://{host}/{dataset}/{bin}'

    print(f'Downloading {bin} from {host} ...')
    with ifcb.open_url(url) as b:
        zip_filename = f'{b.lid}.zip'
        os.makedirs(output_dir, exist_ok=True)
        zip_path = os.path.join(output_dir, zip_filename)
        with ZipFile(zip_path, 'w') as fout:
            features_url = f"{url}_features.csv"

            features_values = pd.read_csv(features_url, index_col='roi_number')

            records = []

            for roi_number, image_data in tqdm(list(b.images.items()), desc='Processing images'):

                features = features_values.loc[roi_number]

                object_id = ifcb.Pid(b.lid).with_target(roi_number)
                img_file_name = f'{object_id}.png'

                record = {
                    'sample_id': b.lid,
                    'object_id': object_id,
                    'img_file_name': img_file_name,
                    'object_date': b.timestamp.strftime('%Y%m%d'),
                    'object_time': b.timestamp.strftime('%H%M%S'),
                }

                for feature, value in features.items():
                    record[f'object_{feature}'] = float(value)

                records.append(record)

                image = Image.fromarray(image_data)
                
                # Convert the image to bytes
                img_byte_arr = BytesIO()
                image.save(img_byte_arr, format='PNG')
                img_byte_arr = img_byte_arr.getvalue()
                
                # Write the bytes to the zip file
                fout.writestr(img_file_name, img_byte_arr)
        
            df = pd.DataFrame(records)
            buffer = BytesIO()
            buffer.write('\t'.join(df.columns).encode() + b'\n')
            buffer.write('\t'.join('[t]' for _ in range(3)).encode() + b'\t')
            buffer.write('\t'.join('[f]' for _ in range(len(df.columns) - 3)).encode() + b'\n')
            df.to_csv(buffer, sep='\t', index=False, header=None)

            fout.writestr('ecotaxa_metadata.tsv', buffer.getvalue())

if __name__ == '__main__':
    # first positional argument is bin id
    parser = argparse.ArgumentParser()
    parser.add_argument('bin', help='bin id')
    # optional params provided for dataset and host
    parser.add_argument('--dataset', help='dataset id', default=DEFAULT_DATASET)
    parser.add_argument('--host', help='host', default=DEFAULT_HOST)
    # optional output directory
    parser.add_argument('--output', help='output directory', default=OUTPUT_DIR)
    #
    args = parser.parse_args()
    main(args.host, args.dataset, args.bin, args.output)