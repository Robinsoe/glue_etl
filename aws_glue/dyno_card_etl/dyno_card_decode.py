
import awswrangler as wr
import array
import boto3


meta_columns = ['NodeID', 'Date', 'CardType', 'SPM', 'StrokeLength', 'Runtime', 'LoadLimit', 'PositionLimit', 'Saved',
                'MalLoadLimit', 'MalPositionLimit', 'Area', 'AreaLimit', 'LoadLimit2', 'PositionLimit2',
                'LoadSpanLimit', 'HiLoadLimit', 'LoLoadLimit', 'CardArea', 'FillBasePct', 'Fillage', 'CauseID',
                'AnalysisDate', 'SecondaryPumpFillage']

card_columns = ['SurfaceCardB', 'DownholeCardB', 'PredictedCardB',
                  'TorquePlotMinEnergyB', 'TorquePlotMinTorqueB', 'TorquePlotCurrentB',
                  'POCDownholeCardB', 'PermissibleLoadUpB', 'PermissibleLoadDownB']


def convert_byte_arrays_of_floats(byte_array):
    if byte_array is None:
        return None
    else:
        return array.array('f', byte_array)

def split_load_and_position(an_array):
    if an_array is None:
        return None, None
    else:
        array_length = len(an_array)
        split = array_length / 2
        load_array = an_array[0:int(split)]
        position_array = an_array[int(split):]
        return load_array, position_array

def round_elements_to_2_decimals(an_array):
    if an_array is None:
        return []
    else:
        return ['%.2f' % elem for elem in an_array]

def clear_empty_lists(a_list):
    if a_list == '[[], []]':
        return None
    else:
        return a_list

if __name__ == '__main__':

    bucket_name = 'xspoc-high-res-glue'
    prefix = 'tblcarddata'
    s3 = boto3.resource('s3', region_name='us-west-2', verify=False)
    bucket = s3.Bucket(bucket_name)
    objs = bucket.objects.filter(Prefix=prefix)

    for obj in objs:
        print(obj.key)
        key = obj.key
        # key = 'tblcarddata/ckcwsqlb/bkxspoc/2020-11-09/part-00000-635ac827-3305-46a8-a8aa-7685ced3f5f5-c000.snappy.parquet'
        # key = 'tblcarddata/ckcwsqlb/ekxspoc/2020-11-12/part-00001-cec8e2ad-4e96-4bd2-8a5d-c004ef71992f-c000.snappy.parquet'
        path = 's3://' + bucket_name + '/' + key

        if path.endswith('.parquet'):
            new_key = 'tblcarddata_decoded/' + key.split('/')[-4] + '/' + key.split('/')[-3] + '/' + key.split('/')[-2] + '/' + key.split('/')[-1]
            new_path = 's3://' + bucket_name + '/' + new_key

            # Check if new key already exists, if it doesn't continue
            try:
                object = s3.Object(bucket_name, new_key).load()
            except:
                # Retrieving the df directly from Amazon S3
                df = wr.s3.read_parquet(path=path)

                # Decode load and postion
                for c in card_columns:
                    c = c.lower()
                    pl_array = []
                    for i in range(len(df)):
                        decoded = convert_byte_arrays_of_floats(df.loc[i, c])
                        load, position = split_load_and_position(decoded)
                        position = round_elements_to_2_decimals(position)
                        load = round_elements_to_2_decimals(load)
                        coordinate = clear_empty_lists(str([load, position]))
                        pl_array.append(coordinate)
                    df[c] = pl_array
                    df[c] = df[c].astype(str)

                # Push to s3
                if not df.empty:
                    wr.s3.to_parquet(df=df, path=new_path)

    print('Finish')
