# -*- coding: utf-8 -*-

"""
IMPORTANT !!!
-------------
    - Variable "a" is used for debugging purposes
"""
import os
import pathlib
import warnings
from collections import defaultdict
from copy import deepcopy
from datetime import time, date, datetime

# import Levenshtein
from Levenshtein import distance
import numpy as np
import pandas as pd
import progressbar
import regex as re

from fuzzywuzzy import process, fuzz
from pandas import Timestamp, DataFrame
from text_unidecode import unidecode

warnings.filterwarnings("ignore", category=FutureWarning,)
DEBUG = 0

def main():
    # TODO uncomment necessary rows!!
    working_path, files = folder_info()
    inputted_data, similarity_score = define_files(working_path, files)
    frame_with_result = process_files(inputted_data, similarity_score)
    write_result(frame_with_result, inputted_data)


def folder_info():
    os.system('cls' if os.name == 'nt' else 'clear')
    if DEBUG:
        print('DEBUG MODE')
    working_dir = pathlib.Path(__file__).parent.absolute()
    print(f'\nYou are here: {working_dir}')
    file_list = [path.name for path in working_dir.glob('*') if path.is_file() and path.name.endswith(".xlsx")]

    print('\nFiles from folder:')
    for i, file_ in enumerate(file_list):
        print(f'{i+1}: {file_}')
    return str(working_dir).replace('\\', '/') + '/', file_list


def define_files(path, files):
    path_is_correct = True
    while True:
        main_file = input(
            '\nType or copy and paste below the name or ID of main file\n'
            '\t(name must include file format)\n'
            '\n>>> '
        )

        secondary_file = input(
            '\nNow type or copy and paste below the name or ID of secondary file\n'
            '\t(name must include file format)\n'
            '\n>>> '
        )
        if main_file.isnumeric() and int(main_file)-1 <= len(files):
            main_file = files[int(main_file)-1]
        if secondary_file.isnumeric() and int(secondary_file)-1 <= len(files):
            secondary_file = files[int(secondary_file)-1]

        two_files = [path.strip() + f_name.strip() for f_name in [main_file, secondary_file]]

        for file_path in two_files:
            if not os.path.isfile(file_path):
                path_is_correct = False
                print(f'Invalid path: {file_path}! Please, try again')

        if path_is_correct:
            similarity_score = int(input(
                'Type similarity score between names in files (skip to set 90 as a default): '
            ) or 90)
            break

    return two_files, similarity_score


def compare_headers(column_1_names, column_2_names) -> Exception or None:
    """Takes two datasets column names as lists and compares them"""

    all_is_True = column_1_names.isin(column_2_names)
    if not all(all_is_True):
        print(column_1_names.array)
        print(column_2_names.array)
        raise Exception('\nColumn names are not the same in your .xlsx files')
    return None


def process_files(filenames, similarity_score):
    matched_rows_count = 0
    new_rows_count = 0
    unmatched_titles_count = 0

    first_df = pd.read_excel(filenames[0])
    second_df = pd.read_excel(filenames[1])

    # first_df.fillna('', inplace=True)
    # second_df.fillna('', inplace=True)

    compare_headers(first_df.columns, second_df.columns)

    # group frames by title
    grouped_first_df = first_df.groupby(by=first_df['Presentation Title'], sort=False, dropna=False)
    grouped_second_df = second_df.groupby(by=second_df['Presentation Title'], sort=False, dropna=False)
    permission_for_rewriting = False
    column_numbers = []

    first_df_grouped_to_list = [list(fr) for fr in grouped_first_df]
    presentations_names_list_firs_df = [
        re.sub(r'[^\p{L}\p{N}]+', '', unidecode(name[0]).lower()).replace(' ', '')
        if isinstance(name[0], str) else 'No_name'
        for name in first_df_grouped_to_list
    ]

    print('\n\n')
    widgets = [
        ' [', progressbar.Timer(), '] ',
        progressbar.Bar(),
        progressbar.Percentage(),
        ' (', progressbar.ETA(), ') ',
    ]
    bar = progressbar.ProgressBar(maxval=grouped_second_df.ngroups, widgets=widgets).start()
    for i, from_second_df_frame in enumerate(grouped_second_df):
        data_from_second_df_frame = from_second_df_frame[1]

        # for debugging purposes
        # if 'MILO/ENGOT-OV11: PHASE-3 STUDY OF BINIMETINIB VERSUS PHYSICIAN’S' in from_second_df_frame[0]:
        #     a = ''

        frame_name = re.sub(r'[^\p{L}\p{N}]+', '', unidecode(from_second_df_frame[0]).lower()).replace(' ', '') \
            if isinstance(from_second_df_frame[0], str) else 'No_name'  # only letters from name
        match_frame_name = [f_name for f_name in presentations_names_list_firs_df if distance(f_name, frame_name) <= 3]
        main_frame_index = presentations_names_list_firs_df.index(match_frame_name[0]) if match_frame_name else int()

        if match_frame_name and frame_name != 'No_name':
            frame_names_from_main_df = [
                name[0].strip()  # from row
                for name in first_df_grouped_to_list[main_frame_index][1].values.tolist()
            ]
            main_names_copy = frame_names_from_main_df.copy()

            # Adding indexes to fix EXACT duplicates overlapping, Gemini told that that's the most efficient way
            name_to_indices_map = defaultdict(list)
            for _i, name in enumerate(frame_names_from_main_df):
                name_to_indices_map[name].append(_i)

            for small_secondary_frame in data_from_second_df_frame.values:
                try:
                    frame_name_from_secondary_frame = small_secondary_frame.tolist()[0].strip()
                except AttributeError:
                    print(small_secondary_frame)
                    frame_name_from_secondary_frame = small_secondary_frame.tolist()[0]

                # Extract almost exact copy or best match,
                # e.g. "P. Yung" from ['N. Yung', 'P. Yung-Mayhem', 'P. Yung'] would match 'P. Yung-Mayhem' on set
                best_match = (process.extractOne(
                    frame_name_from_secondary_frame, main_names_copy, scorer=fuzz.ratio, score_cutoff=98) or
                              process.extractOne(
                    frame_name_from_secondary_frame, main_names_copy, scorer=fuzz.token_set_ratio, score_cutoff=similarity_score))
                value_to_append = pd.Series(small_secondary_frame, index=first_df.columns)
                # if secondary_frame_in_main_df['similarity'] >= similarity_index:
                if best_match:
                    matched_rows_count += 1
                    # do something if items are similar
                    # print(f"similarity: {secondary_frame_in_main_df}")
                    # os.system('cls' if os.name == 'nt' else 'clear')
                    if DEBUG or not permission_for_rewriting:
                        print(f'>This title matched\n'
                              f'\t{from_second_df_frame[0]}')
                    # Old way of getting index
                    # index_of_el_in_main_df = frame_names_from_main_df.index(best_match[0])

                    # New way
                    index_of_el_in_main_df = name_to_indices_map[best_match[0]].pop(0)
                    small_main_frame = first_df_grouped_to_list[main_frame_index][1].values[index_of_el_in_main_df]
                    rewritten_data = rewrite_data_in_small_main_fr( list(first_df.columns),
                        small_main_frame, small_secondary_frame, permission_for_rewriting, column_numbers)
                    np_array_from_df = first_df_grouped_to_list[main_frame_index][1].values
                    np_array_from_df[index_of_el_in_main_df] = rewritten_data[0]
                    permission_for_rewriting = True if rewritten_data[1].lower() == 'a' else False
                    column_numbers = rewritten_data[2] if rewritten_data[1].lower() == 'a' else []
                    #  save rewritten frame
                    first_df_grouped_to_list[main_frame_index][1] = pd.DataFrame(
                        np_array_from_df, columns=first_df.columns)

                    # Delete to bypass duplicates
                    main_names_copy.remove(best_match[0])
                else:
                    new_rows_count += 1
                    # add row to matched frame
                    if DEBUG or not permission_for_rewriting:
                        print(f'>Added new row to the file\n'
                              f'\t{from_second_df_frame[0]}')
                        if best_match:
                            print(f"{'>>> Max. found similarity:':<30} {best_match[1]:<2}%\n"
                                  f"{'>>> From first file:':<30} {frame_name_from_secondary_frame:<15}\n"
                                  f"{'>>> From second file:':<30} {best_match[0]:<15}")
                            print(f"{'>>> No similarities found:':<30} \n"
                                  f"{'>>> From first file:':<30} {frame_name_from_secondary_frame:<15}\n")

                        print('- '*30)
                    # a = ''
                    first_df_grouped_to_list[main_frame_index][1] = first_df_grouped_to_list[main_frame_index][1]._append(value_to_append, ignore_index=True)
                # else:
                #     # !!! for exceptions
                #     print('Something went wrong in matching frames name!')
                #     a = ''
                # a = ''
        elif not match_frame_name and frame_name:
            unmatched_titles_count += 1
            if DEBUG or not permission_for_rewriting:
                print(f'>This title haven`t matched\n{from_second_df_frame[0]}')
            first_df_grouped_to_list.append(from_second_df_frame)
        elif match_frame_name and frame_name == 'No_name':  # for moderators without 'Presentation Titles'
            a = ''
            # TODO
            #  for moderators
            # b = first_df_grouped_to_list[main_frame_index][1]
            # frame_names_from_main_df = [
            #     {'first_letter': el[1][0], 'list_from_names': list_from_names(el[1])}
            #     for el in first_df_grouped_to_list[main_frame_index][1].values.tolist()
            # ]
            # for small_secondary_frame in data_from_second_df_frame.values:
            #     frame_name_from_secondary_frame = {
            #         'first_letter': small_secondary_frame.tolist()[0][0],
            #         'list_from_names': list_from_names(small_secondary_frame.tolist()[0])
            #     }
            #     similarity_index = 0.5
            #     secondary_frame_in_main_df = match_items(
            #         frame_names_from_main_df, frame_name_from_secondary_frame, similarity_index)
            #     value_to_append = pd.Series(small_secondary_frame, index=first_df.columns)
            # d = [el[0] for el in first_df_grouped_to_list[main_frame_index][1].values.tolist()]
            first_df_grouped_to_list.append(from_second_df_frame)
        else:
            # !!! for exceptions
            print('Something went wrong in matching titles!')
            a = ''
        bar.update(i)
    bar.finish()
    main_df = pd.concat(
        [frame_in_list[1] for frame_in_list in first_df_grouped_to_list],
        ignore_index=True,
        join='inner'
    )

    print()
    print('Number of matched rows:', matched_rows_count)
    print('Number of new rows added:', new_rows_count)
    print('Number of unmatched titles:', unmatched_titles_count)

    return main_df


def rewrite_data_in_small_main_fr(columns, from_first_fr, from_second_fr, rewrite_all, columns_to_rewrite):
    num_columns = 3 if len(columns) > 10 else 2
    num_rows = (len(columns) + num_columns - 1) // num_columns

    input_string = "In what column do you want rewrite data?\nPress the appropriate number:\n"

    for i in range(num_rows):
        row = []
        for j in range(num_columns):
            index = i + j * num_rows
            if index < len(columns):
                row.append(f"{index + 1} - '{columns[index]}'".ljust(44))
            else:
                row.append("")
        input_string += "    ".join(row) + "\n"
    input_string += "\n0 - Don't overwrite any column\n"
    input_string += "If you want to rewrite more than one, use a comma for separating column numbers\n"


    while not rewrite_all:
        columns_to_rewrite = input(input_string).split(',')
        rewrite = input("""
    Make change for one case, press - 'O'
    Make changes for all cases, press - 'A'
        : """)

        from_first_fr, rewrite, columns_to_rewrite = rewrite_rows(columns_to_rewrite, rewrite, from_first_fr, from_second_fr)
        return from_first_fr, rewrite, columns_to_rewrite
    else:
        rewrite = 'A'
        from_first_fr, rewrite, columns_to_rewrite = rewrite_rows(columns_to_rewrite, rewrite, from_first_fr,
                                                                  from_second_fr)
        return from_first_fr, rewrite, columns_to_rewrite

def rewrite_rows(columns_to_rewrite, rewrite, from_first_fr, from_second_fr):
    # from_first_fr = np.vectorize(process_time, otypes=[object])(from_first_fr)
    # from_second_fr = np.vectorize(process_time, otypes=[object])(from_second_fr)
    if not '0' in columns_to_rewrite:
        for c_index in columns_to_rewrite:
            data_from_second_fr = from_second_fr[int(c_index.strip()) - 1]
            if data_from_second_fr and not pd.isnull(data_from_second_fr):
                if isinstance(data_from_second_fr, str):
                    data_from_second_fr = data_from_second_fr.strip()
                from_first_fr[int(c_index.strip()) - 1] = data_from_second_fr
                # elif isinstance(data_from_second_fr, time):
                #     from_first_fr[int(c_index.strip()) - 1] = data_from_second_fr.strftime("%H:%M")
                # elif isinstance(data_from_second_fr, Timestamp):
                #     from_first_fr[int(c_index.strip()) - 1] = data_from_second_fr.strftime("%Y-%m-%d")

    return from_first_fr, rewrite, columns_to_rewrite

def write_result(result: DataFrame, input_file_names):
    output_file_name = f'{"_&_".join([f.rsplit("/")[-1].rsplit(".", 1)[0] for f in input_file_names])}.xlsx'
    # Converting to objects, so I can drop Nat, None, NaN values
    result = result.astype(object).fillna('')
    try:
        result['Date'] = pd.to_datetime(result['Date'], errors='coerce')
        result['Start Time'] = result['Start Time'].apply(lambda x: x.strftime("%H:%M") if isinstance(x, (datetime, time)) else x)
        result['End Time'] = result['End Time'].apply(lambda x: x.strftime("%H:%M") if isinstance(x, (datetime, time)) else x)
    except KeyError as e:
        print('Couldn\'t find field: ', e)

    with pd.ExcelWriter(output_file_name,
                        engine='xlsxwriter',
                        engine_kwargs={'options': {'strings_to_urls': False}},
                        date_format="YYYY-MM-DD",
                        datetime_format="YYYY-MM-DD") as writer:
        # Write the DataFrame to excel, starting from the second row and without the default header
        result.to_excel(writer, sheet_name='Sheet1', startrow=1, header=False, index=False)

        # Get the xlsxwriter workbook and worksheet objects
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']

        header_format = workbook.add_format({
            'bold': True,
            'align': 'center',
            'valign': 'vcenter',
            'fg_color': '#9fc5e8',
            'border': 1,
            'text_wrap': True,
            'font_size': 10
        })

        # Write the column headers
        for col_num, value in enumerate(result.columns.values):
            worksheet.write(0, col_num, value, header_format)

        # Adjusting column width for better readability
        worksheet.set_column(0, 0, 25)  # Name
        worksheet.set_column(1, 1, 35)  # Affiliation
        worksheet.set_column(2, 2, 15)  # Role
        worksheet.set_column(3, 3, 10)  # Email
        worksheet.set_column(4, 4, 40)  # Session name
        worksheet.set_column(5, 5, 10)  # Description
        worksheet.set_column(6, 6, 40)  # Title
        worksheet.set_column(7, 7, 10)  # Abstract
        worksheet.set_column(8, 8, 10)  # Abstract URL
        worksheet.set_column(9, 9, 5)  # Video URL
        worksheet.set_column(10, 10, 12)  # Room
        worksheet.set_column(11, 11, 12)  # Date
        worksheet.set_column(12, 12, 7)  # Start Time
        worksheet.set_column(13, 13, 7)  # End Time

    print(f'\n\nFile "{output_file_name}" is successful written and saved.')


if __name__ == "__main__":
    main()
