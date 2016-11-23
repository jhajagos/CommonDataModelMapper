

import json
import glob
import os

def main(directory, glob_pattern, combined_file_name):
    glob_results = glob.glob(os.path.join(directory, glob_pattern))
    files_to_combine = [os.path.split(gr)[-1] for gr in glob_results]

    files_to_combine.sort()

    combined_file_name_path = os.path.join(directory, combined_file_name)

    with open(combined_file_name_path, "wb") as fw:
        i = 0
        for part_file_name in files_to_combine:

            part_file_name_path = os.path.join(directory, part_file_name)
            with open(part_file_name_path, "rb") as f:
                if i > 0:
                    f.next() # skip the header

                for line in f:
                    fw.write(line)

            i += 1


if __name__ == "__main__":
    main("E:\\external\\healtheanalytics\\hf_inpatient_201601\\input\\", "PH_F_Result.*.csv", "PH_F_Result.csv")
