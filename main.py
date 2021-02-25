import sys
import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
import pandas as pd
import openpyxl
import pathlib
import os

parent_path = pathlib.Path(__file__).parent.absolute()

NAMED_RANGES = [
    {
        "input": "Table_1",
        "output": "Table_1_Output"
    },
    {
        "input": "Table_2",
        "output": "Table_2_Output"
    },
    {
        "input": "Table_3",
        "output": "Table_3_Output"
    }
]

df_dict = {}

def process_workbook(event):
    """copies the data in import range and adds them to a dictionary of pandas dataframes"""
    full_filepath = os.path.join(parent_path, event.src_path)
    base_filename = os.path.basename(full_filepath)
    print(f"file added: {full_filepath}")
    print("Processing input file...")
    # Open Workbook using openpyxl
    wkb = openpyxl.load_workbook(filename=f"{full_filepath}", data_only=True)  # specifying data_only=True will return value rather than formula

    for entry in NAMED_RANGES:
        input_range = wkb.defined_names[entry["input"]]
        # Check if range found
        if input_range is None:
            raise ValueError(
                'Range "{}" not found in workbook "{}".'.format(input_range, event.src_path)
            )
        # Port named range into a pandas dataframe
        input_sheetname = input_range.attr_text.split("!")[0]
        input_rng_dest = input_range.destinations
        for sheetname, coord in input_rng_dest:
            records = [[cell.value for cell in row] for row in wkb[sheetname][coord]]
            input_df = pd.DataFrame.from_records(records)
            df_dict[base_filename + "|" + input_range.name] = input_df


# -----------------------------------------------------------------------------------------------------------------
        # To revisit at later date

        # output_range = wkb.defined_names[entry["output"]]
        # # Check if range found
        # if output_range is None:
        #     raise ValueError(
        #         'Range "{}" not found in workbook "{}".'.format(output_range, event.src_path)
        #     )
        # output_sheetname, rng_output = output_range.attr_text.split("!")
        # output_sheetname = output_range.attr_text.split("!")[0]
        # wks_output = wkb[output_sheetname]
        # nm_output = output_range.name
        # wks_output[nm_output] = wks_input[nm_input].values
        # wks_output[output_range.attr_text.split("!")[1]] = wks_input[input_range.attr_text.split("!")[1]].values
        # # -------------------------------------------------------------------------------------------------------
        # # output_range
        # # -------------------------------------------------------------------------------------------------------
    # # Save workbook
    output_filepath = os.path.join(parent_path, "/file_outputs/output_file_1.xlsx")
    #wkb.save(output_filepath)

    wkb.close()

    for key in df_dict.keys():
        print(key)
        print(df_dict[key])
# -----------------------------------------------------------------------------------------------------------------
    print(f"df_dict now has {len(df_dict)} data frames.")
    print("File Processed.")


if __name__ == "__main__":
    patterns = ["*.xls*"]
    ignore_patterns = ""
    ignore_directories = False
    case_sensitive = True
    my_event_handler = PatternMatchingEventHandler(patterns, ignore_patterns, ignore_directories, case_sensitive)
    my_event_handler.on_created = process_workbook

    path = full_filepath = os.path.join(parent_path, "file_inputs")   # ".\\file_inputs"
    go_recursively = True
    my_observer = Observer()
    my_observer.schedule(my_event_handler, path, recursive=go_recursively)
    my_observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        my_observer.stop()
        my_observer.join()
