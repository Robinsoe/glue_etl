import json
import array
import awswrangler as wr

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


def lambda_handler(event, context):
    # try:

    # Create s3 path
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    path = 's3://' + bucket_name + '/' + key

    # Retrieving the df directly from Amazon S3
    df = wr.s3.read_parquet(path=path)

    # Decode load and postion
    for c in card_columns:
        c = c.lower()
        pl_array = []
        for i in range(len(df)):
            load, position = split_load_and_position(convert_byte_arrays_of_floats(df.loc[i, c]))
            position = round_elements_to_2_decimals(position)
            load = round_elements_to_2_decimals(load)
            coordinate = clear_empty_lists(str([load, position]))
            pl_array.append(coordinate)
        df[c] = pl_array
        df[c] = df[c].astype(str)

    # Push to s3
    key = key.split('/')[-4] + '/' + key.split('/')[-3] + '/' + key.split('/')[-2] + '/' + key.split('/')[-1]
    path = 's3://' + bucket_name + '/tblcarddata_decoded/' + key
    wr.s3.to_parquet(df=df, path=path)

    # except:
    #     print('glue_etl failed: ' + key)

    return event