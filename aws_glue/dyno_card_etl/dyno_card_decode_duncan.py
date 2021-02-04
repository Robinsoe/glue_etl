import pyodbc
import numpy as np
import struct
import array
import pickle
import matplotlib.pyplot as plt
import multiprocessing as mp
from PIL import Image

sql_string_dyna_table_size = ("SELECT COUNT(*) FROM dbo.tbl_Well_Card_Hist ;")

sql_string_dyna_data_rows_numbered = ("SELECT " +
                                      "ROW_NUMBER() OVER (ORDER BY NodeID, Date) AS MyRowNumber, " +
                                      "NodeID, Date, CardType, SurfaceCardB, DownholeCardB, PredictedCardB, TorquePlotMinEnergyB, " +
                                      "TorquePlotMinTorqueB, TorquePlotCurrentB, POCDownholeCardB, PermissibleLoadUpB, PermissibleLoadDownB " +
                                      "FROM " +
                                      "dbo.tbl_Well_Card_Hist ")


def sql_string_pull_dyna_data(from_range, to_range):
    sql_string = (
            "SELECT " +
            "NodeID, Date, CardType, SurfaceCardB, DownholeCardB, PredictedCardB, TorquePlotMinEnergyB, " +
            "TorquePlotMinTorqueB, TorquePlotCurrentB, POCDownholeCardB, PermissibleLoadUpB, PermissibleLoadDownB " +
            "FROM " +
            "(" + sql_string_dyna_data_rows_numbered + ") tbl " +
            "WHERE MyRowNumber BETWEEN " + from_range + " AND " + to_range + ";")
    return sql_string


sql_string_insert_decoded_dyna_data = ("insert into " +
                                       "dbo.tbl_Well_Card_Hist_Decoded " +
                                       "(" +
                                       "node_id, plot_date, card_type, SurfaceCardB, DownholeCardB, PredictedCardB, " +
                                       "TorquePlotMinEnergyB, TorquePlotMinTorqueB, TorquePlotCurrentB, POCDownholeCardB, " +
                                       "PermissibleLoadUpB, PermissibleLoadDownB " +
                                       ")" +
                                       "values(?,?,?,?,?,?,?,?,?,?,?,?)")

bda_connection_string = ("Driver={SQL Server};Server=ckcwbda2;Database=BDADB; Trusted_Connection=yes")


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


def get_data_with_sql(sql_string, connection_string):
    if sql_string is None or connection_string is None:
        return None
    else:
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        cursor.execute(sql_string)
        data_table = list(cursor)
        cursor.close()
        connection.close()
        return data_table


def decode_dyna_card_data(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12):
    dino_array = [f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12]
    output = []
    for z in range(0, 3):
        output.append(dino_array[z])
    for x in range(3, 12):
        load, position = split_load_and_position(convert_byte_arrays_of_floats(dino_array[x]))
        position = round_elements_to_2_decimals(position)
        load = round_elements_to_2_decimals(load)
        coordinate = clear_empty_lists(str([load, position]))
        output.append(coordinate)
    return output


def run_in_parallel(num_of_strings, funct_to_run, a_data_table, para_chunk_size):
    results = []
    pool = mp.Pool(processes=num_of_strings)
    results = pool.starmap(funct_to_run, a_data_table, para_chunk_size)
    return results


def insert_data_with_sql(data_table_to_insert, db_connection_string, sql_insert_string):
    conn = pyodbc.connect(db_connection_string)
    cursor = conn.cursor()
    cursor.executemany(sql_insert_string, data_table_to_insert)
    cursor.commit()
    cursor.close()
    conn.close()


def get_table_size():
    table_size_list = get_data_with_sql(sql_string_dyna_table_size, bda_connection_string)
    table_size_int = table_size_list[0][0]
    return table_size_int


def pull_and_insert_dyna_data_in_chunks(batch_chunk_size):
    table_size = get_table_size()
    for i in range(0, table_size, batch_chunk_size):
        from_range = str(i)
        to_range = str(i + batch_chunk_size)
        dyna_table = get_data_with_sql(sql_string_pull_dyna_data(from_range, to_range), bda_connection_string)
        results = run_in_parallel(12, decode_dyna_card_data, dyna_table, 10000)
        insert_data_with_sql(results, bda_connection_string, sql_string_insert_decoded_dyna_data)


if __name__ == '__main__':
    # pull_and_insert_dyna_data_in_chunks(100000)
    pull_and_insert_dyna_data_in_chunks(10)

# ------------possible extra scripts---------------------


############ script for pickling data (still needs work)
# for i in range(3, 12):
#    file_Name = "dino_pkl"
#    fileObject = open(file_Name, 'wb')
#    my_pickle = pickle.dump(output[i],fileObject)
#    output.append(my_pickle)
#    fileObject.close()


############ script for producing a png of dynocard
# for i in range(3, 12)
#    axis_load = output[3][0]
#    axis_position = output[3][1]
#    plt.scatter(axis_position, axis_load)
#    plt.savefig('dino_card.png')
#    img = Image.open('dino_card.png')
#    output.append(img)
